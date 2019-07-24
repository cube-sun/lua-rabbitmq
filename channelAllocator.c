#include "channelAllocator.h"


bool initChannelAllocator(channelAllocator* a, int bottom, int top) {
	a->lastIndex = 0;
	a->loRange = bottom;
	a->hiRange = top;
	a->bitSet = NULL;
	int nb = a->hiRange - a->loRange + 1;
	if (nb < 0) return false;
	size_t size = (size_t)((nb + CHAR_BIT - 1) / CHAR_BIT);
	a->bitSet = (char*)malloc(size);
	if (a->bitSet) {
		memset(a->bitSet, 0x00, size);
		return true;
	}
	
	return false;
}


int getCapacity(channelAllocator*a) {
	return a->hiRange - a->loRange + 1;
}


bool validChannel(channelAllocator* a, int channelNumber) {
	return (channelNumber >= a->loRange) && (channelNumber <= a->hiRange);
}


int genChannel(channelAllocator* a) {
	int setIdx = nextSetBit(a, a->lastIndex);
	if (setIdx < 0) {
		setIdx = nextSetBit(a, 0);
	}
	if (setIdx < 0) return -1;

	a->lastIndex = setIdx;
	BITSET(a->bitSet, setIdx);
	return setIdx + a->loRange;		// output range [bottom, top]
}


bool setChannel(channelAllocator* a, int channelNumber) {
	if (channelNumber < a->loRange || channelNumber > a->hiRange) return false;
	if (BITTEST(a->bitSet, channelNumber - a->loRange)) return false;
	BITSET(a->bitSet, channelNumber - a->loRange);
	return true;
}


bool freeChannel(channelAllocator* a, int channelNumber) {
	if (channelNumber < a->loRange || channelNumber > a->hiRange) return false;
	BITCLEAR(a->bitSet, channelNumber - a->loRange);
	return true;
}


void deinitChannelAllocator(channelAllocator* a) {
	if (a->bitSet) {
		free(a->bitSet);
		a->bitSet = NULL;
	}
}


void printChannelSet(channelAllocator* a) {
	for (int i = a->hiRange - a->loRange; i >= 0; i--) {
		fprintf(stdout, BITTEST(a->bitSet, i) ? "1" : "0");
	}
	printf("\n");
}


// fromIdx -- 开始检查的索引(含)
int nextSetBit(channelAllocator* a, int fromIdx) {
	if (fromIdx >= a->hiRange - a->loRange + 1 || fromIdx < 0) return -1;
	for (int i = fromIdx; i < a->hiRange - a->loRange + 1; ++i) {
		if (!BITTEST(a->bitSet, i)) {
			return i;
		}
	}
	return -1;
}
