
TARGET  = lua-rabbitmq.so
LIBVAR = ./lib/librabbitmq.a  ./lib/libssl.a
INCLUDE = -I./include/rabbitmq-c -I./include/openssl -I../lua/
CFLAGS = -Wall -O2 -g -fno-builtin -std=c99 -fPIC -lrt  --shared
OBJS = lua-rabbitmq.o channelAllocator.o

all : $(OBJS)
	cc -o $(TARGET) $(OBJS) $(CFLAGS) $(LIBVAR)

lua-rabbitmq.o : lua-rabbitmq.c
	cc $(INCLUDE) -c -fPIC -std=c99 lua-rabbitmq.c

channelAllocator.o : channelAllocator.c
	cc -c -fPIC -std=c99  channelAllocator.c

clean :
	rm -f $(OBJS)
	rm -f $(TARGET)
