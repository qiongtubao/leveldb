
LEVEL_DIR := $(shell sh -c 'readlink -f ../../')


latte:
	$(CXX)   latte.cc -I$(LEVEL_DIR)/include $(LEVEL_DIR)/latte/build/libleveldb.a -lsnappy -lz -o latte

clean:
	rm -rf latte