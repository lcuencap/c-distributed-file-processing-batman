# Fleck/Gotham File Processing System

Academic systems programming project in C. Fleck is a client that submits text or media files. Gotham accepts clients, routes each task to an appropriate worker, and tracks worker availability. Enigma handles text; Harley handles media. The components communicate over TCP sockets using a shared frame protocol.

## Architecture

```text
Fleck client -> Gotham server -> Enigma (text)
                              -> Harley (media)
```

Gotham handles worker registration and selects a worker for each file type. Workers send heartbeat messages; the server can choose another available instance when needed. The implementation also uses processes, threads, signals, and shared memory.

## Build and run

Requirements: a POSIX environment, GCC, and GNU Make. The source uses Unix system APIs and is intended for Linux. From the repository root:

```sh
make
./Gotham/gotham Gotham/config.dat
./Worker/Enigma/enigma Worker/Enigma/config.dat
./Worker/Harley/harley Worker/Harley/config.dat
./Fleck/fleck Fleck/config.dat
```

Start each process in a separate terminal, in the order shown. The included configuration uses loopback addresses and example ports. Place your own test files in `Fleck/Testing/`; the example text file is safe to use for a first run. The worker `riddler` directories hold generated output.

This curated copy has not been compiled or run in the current environment. Compiled binaries, logs, and sample media are excluded.

## Academic report

The [original project report](report.pdf) covers design decisions, concurrency, testing, and known limitations. It reflects the course submission; this README describes the curated source currently in the repository.

## Code map

- `Fleck/fleck.c`: client interface, file selection, and transfer.
- `Gotham/gotham.c`: connections, task routing, and worker coordination.
- `Worker/Enigma/enigma.c` and `Worker/Harley/harley.c`: text and media workers.
- `Worker/worker.c`: shared worker support.
- `Utils/trames.c`: frame protocol; `Utils/file_utils.c`: file operations.
- `config.dat` files: local example configuration for each process.

This is a university group project, presented as shared team work. The repository does not claim individual ownership of specific components.
