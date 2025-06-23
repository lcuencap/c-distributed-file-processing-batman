# Compilador i flags
CC = gcc
CFLAGS = -Wall -Wextra -g
LDFLAGS = -lm

# Directori de les carpetes
FLECK_DIR = Fleck
GOTHAM_DIR = Gotham
UTILS_DIR = Utils
WORKER_DIR = Worker
ENIGMA_DIR = $(WORKER_DIR)/Enigma
HARLEY_DIR = $(WORKER_DIR)/Harley

# Arxius objecte
UTILS_OBJS = $(UTILS_DIR)/file_utils.o $(UTILS_DIR)/trames.o $(UTILS_DIR)/so_compression.o
WORKER_OBJS = $(WORKER_DIR)/worker.o

# Targets
all: fleck gotham enigma harley

fleck: $(FLECK_DIR)/fleck.c $(UTILS_OBJS)
	$(CC) $(CFLAGS) -o $(FLECK_DIR)/fleck $(FLECK_DIR)/fleck.c $(UTILS_OBJS) $(LDFLAGS)

gotham: $(GOTHAM_DIR)/gotham.c $(UTILS_OBJS)
	$(CC) $(CFLAGS) -o $(GOTHAM_DIR)/gotham $(GOTHAM_DIR)/gotham.c $(UTILS_OBJS) $(LDFLAGS)

enigma: $(ENIGMA_DIR)/enigma.c $(UTILS_OBJS) $(WORKER_OBJS)
	$(CC) $(CFLAGS) -o $(ENIGMA_DIR)/enigma $(ENIGMA_DIR)/enigma.c $(UTILS_OBJS) $(WORKER_OBJS) $(LDFLAGS)

harley: $(HARLEY_DIR)/harley.c $(UTILS_OBJS) $(WORKER_OBJS)
	$(CC) $(CFLAGS) -o $(HARLEY_DIR)/harley $(HARLEY_DIR)/harley.c $(UTILS_OBJS) $(WORKER_OBJS) $(LDFLAGS)

# Regles per a Utils
$(UTILS_DIR)/file_utils.o: $(UTILS_DIR)/file_utils.c $(UTILS_DIR)/file_utils.h
	$(CC) $(CFLAGS) -c $(UTILS_DIR)/file_utils.c -o $(UTILS_DIR)/file_utils.o

$(UTILS_DIR)/trames.o: $(UTILS_DIR)/trames.c $(UTILS_DIR)/trames.h
	$(CC) $(CFLAGS) -c $(UTILS_DIR)/trames.c -o $(UTILS_DIR)/trames.o

$(UTILS_DIR)/so_compression.o: $(UTILS_DIR)/so_compression.h
	touch $(UTILS_DIR)/so_compression.o

# Regles per a Worker
$(WORKER_DIR)/worker.o: $(WORKER_DIR)/worker.c $(WORKER_DIR)/worker.h
	$(CC) $(CFLAGS) -c $(WORKER_DIR)/worker.c -o $(WORKER_DIR)/worker.o

# Netegen els arxius temporals
clean:
	rm -f $(UTILS_DIR)/*.o $(WORKER_DIR)/*.o
	rm -f $(FLECK_DIR)/fleck $(GOTHAM_DIR)/gotham $(ENIGMA_DIR)/enigma $(HARLEY_DIR)/harley

# Netegen tot
distclean: clean
	rm -f $(FLECK_DIR)/config.dat $(GOTHAM_DIR)/config.dat $(ENIGMA_DIR)/config.dat $(HARLEY_DIR)/config.dat
