// author : wangjingjing

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#include <time.h>
#include <amqp.h>
#include <amqp_framing.h>
#include <amqp_tcp_socket.h>
#include "channelAllocator.h"
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>


const int frame_size = 8*1024;			// now frame size init = 8k, system frame max = 128k
const int heartbeat_interval = 30; 		// second
const int socket_connect_timeout = 500; // millisecond
const char* default_vhost = "/";

static const char* PUBLISH_TEXT_PLAIN_KEY = "text_plain_props";	// store a specified value when publish text/plain content

const char* rabbitmq_metatable = "rabbitmq.MT";

const char* channel_metatable = "channel.MT";	// channel(table)'s metatable
const char* channel_field_mq = "mq_ctx";		// channel table field --> rabbitmq_context
const char* channel_field_channel = "channel";	// channel table field --> channel id
const char* channel_field_noack = "no_ack";		// channel table field --> consumer's prop of no_ack


// channel不是线程安全的，本库使用在skynet框架下，暂不加锁
typedef struct {
	const char* host;
	int port;
	const char* username;
	const char* password;
	const char* vhost;
	amqp_connection_state_t conn;		// pointer
	amqp_socket_t* socket;				// will distroyed by amqp_connection_close
	channelAllocator channelCreator;	// channel id creator
} rabbitmq_context;		// 一次连接的上下文


// util function
const char* error_msg(int err)
{
	if (err < 0) {
		return amqp_error_string2(-err);
	}
	return "SUCCESS";
}


void amqp_error(amqp_rpc_reply_t* rpl, const char* context, luaL_Buffer* b)
{
	bool record_error = false;
	char* err_msg = NULL;
	switch(rpl->reply_type) {
		case AMQP_RESPONSE_NORMAL:
      		return;
      	case AMQP_RESPONSE_NONE:
			err_msg = luaL_prepbuffsize(b, 64);
      		memset(err_msg, 0x00, 64);
      		snprintf(err_msg, 64, "%s: missing RPC reply type!", context);
			luaL_addsize(b, 64);
			record_error = true;
      		break;
      	case AMQP_RESPONSE_LIBRARY_EXCEPTION:
      		err_msg = luaL_prepbuffsize(b, 128);
      		memset(err_msg, 0x00, 128);
      		snprintf(err_msg, 128, "%s: %s", context, amqp_error_string2(rpl->library_error));
			luaL_addsize(b, 128);
			record_error = true;
      		break;
      	case AMQP_RESPONSE_SERVER_EXCEPTION:
      		switch(rpl->reply.id) {
      			case AMQP_CONNECTION_CLOSE_METHOD: {
					amqp_connection_close_t *m = (amqp_connection_close_t*)rpl->reply.decoded;
					size_t size = (size_t)(m->reply_text.len+64);
					err_msg = luaL_prepbuffsize(b, size);
      				memset(err_msg, 0x00, size);
      				snprintf(err_msg, size, "%s: server connection error %d, message: %.*s", 
      					context, m->reply_code, (int)m->reply_text.len, (char*)m->reply_text.bytes);
					luaL_addsize(b, size);
					record_error = true;
					break;
				}
				case AMQP_CHANNEL_CLOSE_METHOD: {
					amqp_channel_close_t *m = (amqp_channel_close_t*)rpl->reply.decoded;
					size_t size = (size_t)(m->reply_text.len+64);
					err_msg = luaL_prepbuffsize(b, size);
		      		memset(err_msg, 0x00, size);
      				snprintf(err_msg, size, "%s: server channel error %d, message: %.*s", 
		      			context, m->reply_code, (int)m->reply_text.len, (char*)m->reply_text.bytes);
					luaL_addsize(b, size);
					record_error = true;
				  	break;
				}
				default:
					err_msg = luaL_prepbuffsize(b, 64);
			      	memset(err_msg, 0x00, 64);
	    		  	snprintf(err_msg, 64, "%s: unknown server error, method id 0x%08X", context, rpl->reply.id);
					luaL_addsize(b, 64);
					record_error = true;
				  	break;
			}
      		break;
	}

	if (!record_error) {
		err_msg = luaL_prepbuffsize(b, 128);
		memset(err_msg, 0x00, 128);
		snprintf(err_msg, 128, "%s: unknown error, reply type(%d), library error(%d), reply method id(0x%08X)",
			context, rpl->reply_type, rpl->library_error, rpl->reply.id);
		luaL_addsize(b, 128);
	}
}


lua_State* mq_new_thread(lua_State* L, int* ref)
{
	int base = lua_gettop(L);
	lua_State* co = lua_newthread(L);	/* if ref failure, it will be auto recycled by GC */
	if (!co) { return NULL; }
	*ref = luaL_ref(L, LUA_REGISTRYINDEX);
	if (*ref == LUA_NOREF) {
		lua_settop(L, base);  /* restore main thread stack */
		return NULL;
	}
	
	lua_settop(L, base);
	return co;
}


void checkChannel(lua_State* L, int index, const char* emsg) {
	if (1 == lua_getmetatable(L, index)) {
		lua_pop(L, 1);
		if (lua_type(L, index) != LUA_TTABLE) {
			luaL_error(L, "%s", emsg);
		}
		/*	{								栈:
 		 *		"no_ack" = xx,				-1
 		 *		"channel" = xx,		===>    -2
 		 *		"mq_ctx" = xx				-3
 		 *	}
 		 */
		lua_getfield(L, index, channel_field_mq);
		if (lua_type(L, -1) != LUA_TUSERDATA) {
			luaL_error(L, "%s", emsg);
		}
		lua_getfield(L, index, channel_field_channel);
		if (lua_type(L, -1) != LUA_TNUMBER) {
			luaL_error(L, "%s", emsg);
		}
		lua_getfield(L, index, channel_field_noack);
		if (lua_type(L, -1) != LUA_TBOOLEAN) {
			luaL_error(L, "%s", emsg);
		}
	} else {
		luaL_error(L, "%s", emsg);
	}
}


// metatable method
static int lmqinfo2string(lua_State* L)
{
	rabbitmq_context** ud = (rabbitmq_context**)luaL_checkudata(L, 1, rabbitmq_metatable);
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	lua_pushfstring(L, "host = %s, port = %d, username = %s, vhost = %s", 
		ctx->host, ctx->port, ctx->username, ctx->vhost);
	return 1;
}


static int lfreeRabbitmq(lua_State* L)
{
	rabbitmq_context** ud = (rabbitmq_context**)luaL_checkudata(L, 1, rabbitmq_metatable);
	if (ud) {
		rabbitmq_context* ctx = *ud;
		if (ctx) {
			free(ctx);	// 连接的关闭和连接对象的销毁由调用者保证，维持实时性，而不是放在GC里
			ctx = NULL;
		}
	}
	return 0;
}


static int lfreeChannel(lua_State* L)
{
	lua_pushnil(L);
	lua_setfield(L, -2, channel_field_mq);
	return 0;
}


// factory function
static int lnewRabbitmq(lua_State* L)
{
	rabbitmq_context *ctx = (rabbitmq_context*)malloc(sizeof(rabbitmq_context));
	if (!ctx) {
		return luaL_error(L, "malloc for rabbitmq context failed!");
	}
	ctx->host = NULL;
	ctx->port = 0;
	ctx->username = NULL;
	ctx->password = NULL;
	ctx->vhost = NULL;
	ctx->conn = NULL;
	ctx->socket = NULL;
	memset(&ctx->channelCreator, 0, sizeof(channelAllocator));
	
	rabbitmq_context **ud = (rabbitmq_context**)lua_newuserdata(L, sizeof(rabbitmq_context*));
	*ud = ctx;
	
	luaL_getmetatable(L, rabbitmq_metatable);
	lua_setmetatable(L, -2);

	return 1;
}


// member method
static int lconf(lua_State* L)
{
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, 1, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, 1, "'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }

	int argc = lua_gettop(L);
	if (!(5 == argc || 6 == argc)) {
		return luaL_error(L, "wrong number of argument = %d, expected 5 or 6", argc);
	}

	ctx->host = luaL_checkstring(L, 2);
	ctx->port = luaL_checkinteger(L, 3);
	ctx->username = luaL_checkstring(L, 4);
	ctx->password = luaL_checkstring(L, 5);
	ctx->vhost = (5 == argc) ? default_vhost : luaL_checkstring(L, 6);

	return 0;		
}


static int lconnect(lua_State* L)
{
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, 1, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, 1, "'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	if (ctx->socket != NULL && ctx->conn != NULL) { 
		amqp_connection_close(ctx->conn, AMQP_REPLY_SUCCESS);
		ctx->socket = NULL;
	}
	if (ctx->conn != NULL) {
		deinitChannelAllocator(&ctx->channelCreator);
		amqp_destroy_connection(ctx->conn);
		ctx->conn = NULL;
	}

	ctx->conn = amqp_new_connection();
	ctx->socket = amqp_tcp_socket_new(ctx->conn);
	if (ctx->socket == NULL) {
		return luaL_error(L, "new a socket failed: %s", error_msg(AMQP_STATUS_SOCKET_ERROR));
	}

	if (!initChannelAllocator(&ctx->channelCreator, 1, AMQP_DEFAULT_MAX_CHANNELS)) {
		return luaL_error(L, "initChannelAllocator failed, memory malloc failed");
	}
	
	struct timeval tv = { 0 };
	tv.tv_usec = socket_connect_timeout * 1000;
	int ret = amqp_socket_open_noblock(ctx->socket, ctx->host, ctx->port, &tv);
	if (AMQP_STATUS_OK != ret) {
		deinitChannelAllocator(&ctx->channelCreator);
		lua_pushfstring(L, "open tcp socket failed(%d) : %s", ret, error_msg(ret));
		return 1;
	}

	amqp_rpc_reply_t rpl = amqp_login(ctx->conn, ctx->vhost, 0, frame_size, heartbeat_interval, 
		AMQP_SASL_METHOD_PLAIN, ctx->username, ctx->password);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		deinitChannelAllocator(&ctx->channelCreator);
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "logging in", &b);
		luaL_pushresult(&b);
		return 1;
	}
	
	lua_pushnil(L);
	return 1;
}


static int lcreateChannel(lua_State* L)
{
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, 1, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, 1, "'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }

	int argc = lua_gettop(L);
	int channel = -1;
	if (1 == argc) {
		channel = genChannel(&ctx->channelCreator);
		if (-1 == channel) {
			lua_pushnil(L);
			lua_pushfstring(L, "channel capacity = %d, is full now, create failed", getCapacity(&ctx->channelCreator));
			return 2;
		}
	} else if (2 == argc) {
		channel = luaL_checkinteger(L, 2);
		if (!setChannel(&ctx->channelCreator, channel)) {
			lua_pushnil(L);
			lua_pushfstring(L, "set channel id = %d failed, invalid channel or channel is exist", channel);
			return 2;
		}
	} else {
		return luaL_error(L, "wrong number of argument = %d, expected 1 or 2", argc);
	}

	amqp_channel_open(ctx->conn, channel);
	amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		freeChannel(&ctx->channelCreator, channel);
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "creating channel", &b);
		lua_pushnil(L);
		luaL_pushresult(&b);
		return 2;
	}
	
	if (1 != argc) { lua_settop(L, 1); }

	/* create channel object */
	lua_newtable(L);
	lua_rotate(L, 1, 1);
	lua_setfield(L, -2, channel_field_mq);

	lua_pushinteger(L, channel);
	lua_setfield(L, -2, channel_field_channel);

	lua_pushboolean(L, 0);
	lua_setfield(L, -2, channel_field_noack);

	luaL_getmetatable(L, channel_metatable);
	lua_setmetatable(L, -2);
	
	lua_pushnil(L);
	return 2;
}


static int ldisConnect(lua_State* L)
{
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, 1, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, 1, "'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }

	if (ctx->socket != NULL) {
		amqp_connection_close(ctx->conn, AMQP_REPLY_SUCCESS);
		amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
		if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
			luaL_Buffer b;
			luaL_buffinit(L, &b);
			amqp_error(&rpl, "closing connection", &b);
			luaL_pushresult(&b);
			return 1;
		}
		deinitChannelAllocator(&ctx->channelCreator);
		ctx->socket = NULL;
	}
	
	if (ctx->conn != NULL) {
		int ret = amqp_destroy_connection(ctx->conn);
		if (AMQP_STATUS_OK != ret) {
			lua_pushfstring(L, "ending connection failed(%d) : %s", ret, error_msg(ret));
			return 1;
		}
		ctx->conn = NULL;
	}

	lua_pushnil(L);
	return 1;
}


static int lexchangeDeclare(lua_State* L)
{
	int argc = lua_gettop(L);
	checkChannel(L, 1, "bad argument #1 to 'exchangeDeclare' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	int channel = luaL_checkinteger(L, -2);
	lua_settop(L, argc);
	if (5 != argc) {
        return luaL_error(L, "wrong number of argument = %d, expected 5", argc);
    }

	const char *exchange = luaL_checkstring(L, 2);
	const char *exchange_type = luaL_checkstring(L, 3);
	int durable = lua_toboolean(L, 4);
	int auto_delete = lua_toboolean(L, 5);

	amqp_exchange_declare(ctx->conn, channel, amqp_cstring_bytes(exchange), 
		amqp_cstring_bytes(exchange_type), 0, durable, auto_delete, 0, amqp_empty_table);
	amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "declaring exchange", &b);
		luaL_pushresult(&b);
		return 1;
	}
	
	lua_pushnil(L);
	return 1;
}


static int lqueueDeclare(lua_State* L)
{
	int argc = lua_gettop(L);
	checkChannel(L, 1, "bad argument #1 to 'queueDeclare' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	int channel = luaL_checkinteger(L, -2);
	lua_settop(L, argc);
	if (5 != argc) {
        return luaL_error(L, "wrong number of argument = %d, expected 5", argc);
    }

	const char *queue = luaL_optstring(L, 2, "");
	int durable = lua_toboolean(L, 3);
	int exclusive = lua_toboolean(L, 4);
	int auto_delete = lua_toboolean(L, 5);

	amqp_bytes_t out_queuename, in_queuename;
	if (queue && *queue == '\0') {
		in_queuename = amqp_empty_bytes;
	} else {
		in_queuename = amqp_cstring_bytes(queue);
	}
	
	amqp_queue_declare_ok_t *r = amqp_queue_declare(ctx->conn, channel, in_queuename, 
		0, durable, exclusive, auto_delete, amqp_empty_table);
	amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "declaring queue", &b);
		lua_pushnil(L);
		luaL_pushresult(&b);
		return 2;
	}
	
	out_queuename = amqp_bytes_malloc_dup(r->queue);
	if (out_queuename.bytes == NULL) {
		lua_pushnil(L);
		lua_pushstring(L, "out of memory while copying queue name");
		return 2;		
	}
	
	lua_pushlstring(L, out_queuename.bytes, out_queuename.len);
	lua_pushnil(L);
	amqp_bytes_free(out_queuename);
	return 2;
}


static int lqueueBind(lua_State* L)
{
	int argc = lua_gettop(L);
	checkChannel(L, 1, "bad argument #1 to 'queueBind' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	int channel = luaL_checkinteger(L, -2);
	lua_settop(L, argc);
	if (4 != argc) {
        return luaL_error(L, "wrong number of argument = %d, expected 4", argc);
    }

	const char* queue = luaL_checkstring(L, 2);
	const char* exchange = luaL_checkstring(L, 3);
	const char* routing_key = luaL_checkstring(L, 4);

	amqp_queue_bind(ctx->conn, channel, amqp_cstring_bytes(queue), 
		amqp_cstring_bytes(exchange), amqp_cstring_bytes(routing_key), amqp_empty_table);
	amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "binding queue", &b);
		luaL_pushresult(&b);
		return 1;
	}
	
	lua_pushnil(L);
	return 1;
}


static int lcloseChannel(lua_State* L)
{
	int argc = lua_gettop(L);	
	checkChannel(L, 1, "bad argument #1 to 'closeChannel' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	int channel = luaL_checkinteger(L, -2);
	lua_settop(L, argc);
	if (1 != argc) {
        return luaL_error(L, "wrong number of argument = %d, expected 1", argc);
    }

	if (!validChannel(&ctx->channelCreator, channel)) {
		lua_pushfstring(L, "close channel id = %d failed, invalid channel", channel);
		return 1;
	}

	amqp_channel_close(ctx->conn, channel, AMQP_REPLY_SUCCESS);
	amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "closing channel", &b);
		luaL_pushresult(&b);
		return 1;
	}
	
	freeChannel(&ctx->channelCreator, channel);
	lua_pushnil(L);
	return 1;
}


static int lbasicQos(lua_State *L)
{
	int argc = lua_gettop(L);
	checkChannel(L, 1, "bad argument #1 to 'basicQos' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	int channel = luaL_checkinteger(L, -2);
	lua_settop(L, argc);	
	if (3 != argc) {
        return luaL_error(L, "wrong number of argument = %d, expected 3", argc);
    }
	
	int prefetch_count = luaL_checkinteger(L, 2);	// uint16_t
	luaL_argcheck(L, 1 <= prefetch_count && prefetch_count <= ((1<<16)-1), 
		3, "prefetch count out of range");
	int global = lua_toboolean(L, 3);
	
	amqp_basic_qos(ctx->conn, channel, 0, prefetch_count, global);
	amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "setting basic qos", &b);
		luaL_pushresult(&b);
		return 1;
	}
	
	lua_pushnil(L);
	return 1;
}


static int lbasicConsume(lua_State* L)
{
	int argc = lua_gettop(L);
	checkChannel(L, 1, "bad argument #1 to 'basicConsume' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	int channel = luaL_checkinteger(L, -2);
	lua_settop(L, argc);
	if (6 != argc) {
        return luaL_error(L, "wrong number of argument = %d, expected 6", argc);
    }

	const char* queue = luaL_checkstring(L, 2);
	const char* consumer_tag = luaL_optstring(L, 3, "");
	int no_local = lua_toboolean(L, 4);
	int no_ack = lua_toboolean(L, 5);
	int exclusive = lua_toboolean(L, 6);
	amqp_bytes_t in_consumer_tag;
	if (consumer_tag && *consumer_tag == '\0') {
		in_consumer_tag = amqp_empty_bytes;
	} else {
		in_consumer_tag = amqp_cstring_bytes(consumer_tag);
	}
	
	amqp_basic_consume(ctx->conn, channel, amqp_cstring_bytes(queue), 
		in_consumer_tag, no_local, no_ack, exclusive, amqp_empty_table);
	amqp_rpc_reply_t rpl = amqp_get_rpc_reply(ctx->conn);
	if (rpl.reply_type != AMQP_RESPONSE_NORMAL) {
		luaL_Buffer b;
		luaL_buffinit(L, &b);
		amqp_error(&rpl, "start consumer", &b);
		luaL_pushresult(&b);
		return 1;
	}
	
	lua_pushvalue(L, 5);
	lua_setfield(L, 1, channel_field_noack);

	lua_pushnil(L);
	return 1;	
}


static int lbasicPublish(lua_State* L)
{
	int argc = lua_gettop(L);
	checkChannel(L, 1, "bad argument #1 to 'basicPublish' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	
	int channel = luaL_checkinteger(L, -2);
	lua_settop(L, argc);
	if (6 != argc) {
        return luaL_error(L, "wrong number of argument = %d, expected 6", argc);
    }

	const char* exchange = luaL_checkstring(L, 2);
	const char* routing_key = luaL_checkstring(L, 3);
	int mandatory = lua_toboolean(L, 4);
	int immediate = lua_toboolean(L, 5);
	const char* body = luaL_checkstring(L, 6);

	lua_pushlightuserdata(L, (void*)&PUBLISH_TEXT_PLAIN_KEY);
	lua_gettable(L, LUA_REGISTRYINDEX);
	amqp_basic_properties_t* props = lua_touserdata(L, -1);
	if (!props) { return luaL_error(L, "lack amqp_basic_properties_t field"); }
	
	int ret = amqp_basic_publish(ctx->conn, channel, amqp_cstring_bytes(exchange),
		amqp_cstring_bytes(routing_key), mandatory, immediate, props, amqp_cstring_bytes(body));
	if (AMQP_STATUS_OK != ret) {
		lua_pushfstring(L, "publishing failed(%d) : %s", ret, error_msg(ret));
		return 1;
	}
	
	lua_pushnil(L);
	return 1;
}


static int finishpcall(lua_State *L, int status, lua_KContext extra) {	/* continuation function */
	if (status != LUA_OK && status != LUA_YIELD) {	/* error? */
		lua_pushboolean(L, 0);  /* first result (false) */
		lua_pushvalue(L, -2);  /* error message */
		return 2; 	/* return false, msg */
	} else {
		int argc = lua_gettop(L);
		if (argc < (int)extra) { 
			return luaL_error(L, "wrong number of argument = %d, expected >= %d", argc, (int)extra); 
		}

		lua_getfield(L, 3, channel_field_mq);
		rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -1, rabbitmq_metatable);
		luaL_argcheck(L, ud != NULL, -1, "channel field mq_ctx: 'rabbitmq' expected");
		rabbitmq_context *ctx = *ud;
		if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }

		lua_getfield(L, 3, channel_field_noack);
		int no_ack = lua_toboolean(L, -1);
		lua_settop(L, argc);
	
		amqp_envelope_t* envelope = lua_touserdata(L, 4);
		if (!envelope) { luaL_error(L, "bad envelope pass to continuation function"); }

		if (!no_ack) {
			int ret = amqp_basic_ack(ctx->conn, envelope->channel, envelope->delivery_tag, 0);
			if (AMQP_STATUS_OK != ret) {
				lua_pushboolean(L, 0);  /* first result (false) */
				lua_pushfstring(L, "basic ack failed(%d):%s, delivery:%u, exchange:%.*s, routingkey:%.*s", 
					ret, error_msg(ret), (unsigned)envelope->delivery_tag, (int)envelope->exchange.len,
					(char*)envelope->exchange.bytes, (int)envelope->routing_key.len, (char*)envelope->routing_key.bytes);
				return 2;
			}
		}
		amqp_destroy_envelope(envelope);
		
		/* remove non result stacks, to ensure the first result is pcall execute status */
		int nRet = argc - (int)extra;
		lua_rotate(L, 1, nRet);
		for (int i = argc; i > nRet; --i)
			lua_remove(L, i);
		return nRet;	/* return all results */
	}
}


static int lwait(lua_State* L)
{
	int argc = lua_gettop(L);
	checkChannel(L, 1, "bad argument #1 to 'wait' (channel expected)");
	rabbitmq_context **ud = (rabbitmq_context**)luaL_checkudata(L, -3, rabbitmq_metatable);
	luaL_argcheck(L, ud != NULL, -3, "channel field mq_ctx: 'rabbitmq' expected");
	rabbitmq_context *ctx = *ud;
	if (ctx == NULL) { return luaL_error(L, "create rabbitmq first"); }
	lua_settop(L, argc);
	if (argc < 3) {
        return luaL_error(L, "wrong number of argument = %d, expected >= 3", argc);
    }
	int pcall_argc = argc + 1 - 3;	/* calc pcall's argc */

	luaL_checktype(L, 3, LUA_TFUNCTION);	/* check error function */
	lua_rotate(L, 1, -1);	/* 1. move channel to top */
	lua_insert(L, 3);		/* 2. then move channel below all arguments */
	
	lua_pushboolean(L, 1);	/* first result */
	lua_pushvalue(L, 1);	/* function */
	lua_rotate(L, 4, 2);  	/* move them below arguments */
	
	int base = lua_gettop(L);
	for (;;) {
		amqp_maybe_release_buffers(ctx->conn);

		amqp_envelope_t envelope;
		amqp_rpc_reply_t rpl = amqp_consume_message(ctx->conn, &envelope, NULL, 0);
		
		if (AMQP_RESPONSE_NORMAL != rpl.reply_type) {
			amqp_frame_t frame;
			if (AMQP_RESPONSE_LIBRARY_EXCEPTION == rpl.reply_type && 
				AMQP_STATUS_UNEXPECTED_STATE == rpl.library_error) {
				int ret = amqp_simple_wait_frame(ctx->conn, &frame);
				if (AMQP_STATUS_OK != ret) {
					amqp_destroy_envelope(&envelope);
					lua_pushfstring(L, "wait frame err = %s", error_msg(ret));
					return 1;
				}
				if (AMQP_FRAME_METHOD == frame.frame_type) {
					switch (frame.payload.method.id) {
						case AMQP_BASIC_ACK_METHOD:		/* when publishing */
							/* if we've turned publisher confirms on, and we've published a message
							 *  here is a message being confirmed
							 */	
							break;
						case AMQP_BASIC_NACK_METHOD:	/* when publishing */
							/* if we've turned publisher confirms on, and we've published a message
 							 * here is a message not being confirmed
 							 */
							break;
						case AMQP_BASIC_RETURN_METHOD:	/* when publishing */
							/* if a published message couldn't be routed and the mandatory flag was set
 							 * this is what would be returned. The message then needs to be read.
 							 */
							/* we should implement message read in publish endpoint
							 * see about: https://blog.csdn.net/jiao_fuyou/article/details/21594205
							 */
							break;
						case AMQP_CHANNEL_CLOSE_METHOD:
							/* a channel.close method happens when a channel exception occurs, this
 							 * can happen by publishing to an exchange that doesn't exist for example
 							 *
 							 * In this case you would need to open another channel redeclare any queues
 							 * that were declared auto-delete, and restart any consumers that were attached
 							 * to the previous channel
 							 */
							amqp_destroy_envelope(&envelope);
							lua_pushstring(L, "channel close happens, restart any consumers attached to this channel");
							return 1;
						case AMQP_CONNECTION_CLOSE_METHOD:
							/* a connection.close method happens when a connection exception occurs,
 							 * this can happen by trying to use a channel that isn't open for example.
 							 *
 							 * In this case the whole connection must be restarted.
 							 */
							amqp_destroy_envelope(&envelope);
							lua_pushstring(L, "connection close happens, the whole connection must be restarted");
							return 1;
						default:
							fprintf(stderr ,"an unexpected method was received %d\n", frame.payload.method.id);
							break;
					}
				}
			}
			amqp_destroy_envelope(&envelope);	
		} else {
			if (strncmp("quit", (char*)envelope.message.body.bytes, 4) == 0) {
				amqp_destroy_envelope(&envelope);
				break;	
			}

			for (int i = 1; i <= base; i++) {	/* copy for xmove */
				lua_pushvalue(L, i);
			}
			int ref = LUA_REFNIL;
			lua_State* newL = mq_new_thread(L, &ref);
			if (!newL) { return luaL_error(L, "new coroutine failure"); }
			
			lua_xmove(L, newL, base);
			lua_pushlightuserdata(newL, &envelope);
			lua_insert(newL, 4);
			lua_pushlstring(newL, (char*)envelope.message.body.bytes, envelope.message.body.len);
				
			int status = lua_pcallk(newL, pcall_argc, LUA_MULTRET, 2, 4, finishpcall);
			finishpcall(newL, status, 4);
			if (0 == lua_toboolean(newL, 1)) {
				fprintf(stderr, "callback function exec err = %s\n", lua_tostring(newL, -1));
				/* any better way to handle error ? */
			}
			luaL_unref(L, LUA_REGISTRYINDEX, ref);
		}
	}

	lua_settop(L, 0);
	lua_pushnil(L);
	return 1;
}


// load function
LUALIB_API int luaopen_rabbitmq(lua_State* L) {
	luaL_checkversion(L);
	
	// the registry
	// specified props
	static amqp_basic_properties_t text_plain_props;
	text_plain_props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG | AMQP_BASIC_DELIVERY_MODE_FLAG;
	text_plain_props.content_type = amqp_cstring_bytes("text/plain");
	text_plain_props.delivery_mode = AMQP_DELIVERY_PERSISTENT;
	lua_pushlightuserdata(L, (void*)&PUBLISH_TEXT_PLAIN_KEY);
	lua_pushlightuserdata(L, (void*)&text_plain_props);
	lua_settable(L, LUA_REGISTRYINDEX);
  	
	// rabbitmq member functions
	const luaL_Reg rabbitmq_m[] = {
		{ "conf", lconf },
		{ "connect", lconnect },
		{ "createChannel", lcreateChannel },
		{ "disConnect", ldisConnect },
		{ NULL, NULL },
	};

	if (luaL_newmetatable(L, rabbitmq_metatable)) {		/* rabbitmq */
		lua_pushvalue(L, -1);
		lua_setfield(L, -2, "__index");
		
		lua_pushcfunction(L, lfreeRabbitmq);
		lua_setfield(L, -2, "__gc");
		
		lua_pushcfunction(L, lmqinfo2string);
		lua_setfield(L, -2, "__tostring");

		// register metatable method
		luaL_setfuncs(L, rabbitmq_m, 0);
	}
	lua_pop(L, 1);

	// channel member functions
	const luaL_Reg channel_m[] = {
		{ "exchangeDeclare", lexchangeDeclare },
		{ "queueDeclare", lqueueDeclare },
		{ "queueBind", lqueueBind },
		{ "closeChannel", lcloseChannel },
		{ "basicQos", lbasicQos },
		{ "basicConsume", lbasicConsume },
		{ "basicPublish", lbasicPublish },
		{ "wait", lwait },
		{ NULL, NULL },
	};

	if (luaL_newmetatable(L, channel_metatable)) {		/* channel */
		lua_pushvalue(L, -1);
		lua_setfield(L, -2, "__index");
		
		lua_pushcfunction(L, lfreeChannel);
		lua_setfield(L, -2, "__gc");

		luaL_setfuncs(L, channel_m, 0);
	}
	lua_pop(L, 1);
	
	// factory functions
	const luaL_Reg rabbitmq_f[] = {
		{ "newRabbitmq", lnewRabbitmq },
		{ NULL, NULL },
	};

	luaL_newlib(L, rabbitmq_f);
	return 1;
}
