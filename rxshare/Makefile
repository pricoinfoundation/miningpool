# Build librxshare.so. Links statically against the librandomx.a produced
# by the main pricoin build. Override paths if your tree lives elsewhere.

RANDOMX_LIB ?= $(HOME)/bitcoin/build_pric/src/crypto/randomx/librandomx.a
RANDOMX_INC ?= $(HOME)/bitcoin/src/crypto/randomx/src

CC      ?= cc
CXX     ?= c++
CFLAGS  ?= -O3 -g -fPIC -Wall -Wextra -pthread -I$(RANDOMX_INC)
LDLIBS  ?= -lcrypto -pthread

.PHONY: all clean

all: librxshare.so

rxshare.o: rxshare.c rxshare.h
	$(CC) $(CFLAGS) -c -o $@ $<

# Link with the C++ driver so the C++ runtime symbols required by
# librandomx.a get pulled in.
librxshare.so: rxshare.o $(RANDOMX_LIB)
	$(CXX) -shared -pthread -o $@ rxshare.o $(RANDOMX_LIB) $(LDLIBS)

$(RANDOMX_LIB):
	$(error librandomx.a not found at $@; build pricoin first \
	  (cmake --build $(HOME)/bitcoin/build_pric -j) \
	  or set RANDOMX_LIB=/path/to/librandomx.a)

clean:
	rm -f rxshare.o librxshare.so
