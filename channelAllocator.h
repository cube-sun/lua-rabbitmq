// author : wangjingjing

#ifndef	CHANNEL_ALLOCATOR_H
#define CHANNEL_ALLOCATOR_H

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <stdbool.h>
#include <memory.h>

#define BITMASK(b) (1 << ((b) % CHAR_BIT))
#define BITSLOT(b) ((b) / CHAR_BIT)
#define BITSET(a, b) ((a)[BITSLOT(b)] |= BITMASK(b))
#define BITCLEAR(a, b) ((a)[BITSLOT(b)] &= ~BITMASK(b))
#define BITTEST(a, b) ((a)[BITSLOT(b)] & BITMASK(b))
#define BITNSLOTS(nb) ((nb + CHAR_BIT - 1) / CHAR_BIT)

// channel id 生成器
typedef struct {
	int lastIndex;	// 上次分配的索引
	int loRange;
	int hiRange;
	char *bitSet;	// internal range [loRange-1, hiRange), 0表示可用, 1表示占用
} channelAllocator;

// as public:
// init 
bool initChannelAllocator(channelAllocator* a, int bottom, int top);

// check a channel is valid or not
bool validChannel(channelAllocator* a, int channelNumber);

// get channel capacity
int getCapacity(channelAllocator*a);

// generate a channel id, output range [bottom, top]
int genChannel(channelAllocator* a);

// set a channel id is used
bool setChannel(channelAllocator* a, int channelNumber);

// recycle a channel id
bool freeChannel(channelAllocator* a, int channelNumber);

// deinit
void deinitChannelAllocator(channelAllocator* a);

// typically, use for debug, print the bitSet
void printChannelSet(channelAllocator* a);

// as private:
// fromIdx -- 开始检查的索引(含)
int nextSetBit(channelAllocator* a, int fromIdx);


#endif /* CHANNEL_ALLOCATOR_H */
