# Practica SO 2024-25

Aquest projecte està dissenyat per modularitzar i executar diverses funcionalitats de gestió i processament utilitzant quatre mòduls principals: **Fleck**, **Gotham**, **Enigma** i **Harley**. El projecte està estructurat en diverses carpetes per mantenir una organització clara i facilitar el manteniment.

---

## Estructura del Projecte

* Fleck/: Conté el codi del client **Fleck** i els fitxers rellevants per gestionar fitxers de tipus media o text per enviar a Gotham. Aquesta carpeta inclou:

  * `fleck.c`: Codi font del mòdul.
  * `config.dat`: Fitxer de configuració per a Fleck.
  * `Testing/`: Carpeta que conté tots els fitxers que es poden distorsionar (fitxers `.txt`, `.jpg`, `.wav`, etc.).

* Gotham/: Conté el servidor principal que gestiona les connexions amb Fleck i delega tasques als workers. Aquesta carpeta inclou:

  * `gotham.c`: Codi font del servidor Gotham.
  * `config.dat`: Fitxer de configuració per a Gotham.

* Worker/: Carpeta principal per als workers **Enigma** i **Harley**.

  * `worker.c` i `worker.h`: Contenen la lògica compartida per als workers.
  * `Enigma/`:

    * `enigma.c`: Codi font per al worker Enigma (processa fitxers de tipus text).
    * `config.dat`: Fitxer de configuració per a Enigma.
  * `Harley/`:

    * `harley.c`: Codi font per al worker Harley (processa fitxers de tipus media).
    * `config.dat`: Fitxer de configuració per a Harley.

* Utils/: Conté funcionalitats compartides entre els mòduls, com ara la gestió de trames i utilitats de fitxers.

  * `file_utils.c` i `file_utils.h`: Gestó de fitxers.
  * `trames.c` i `trames.h`: Gestó de trames de comunicació.
  * `so_compression.h`: Capçalera precompilada.
  * `sys_params.h`: Paràmetres globals del sistema.

* `makefile`: Arxiu per compilar tots els mòduls.

---

## Execució

Per executar qualsevol mòdul, és necessari especificar el path al binari i el path al fitxer `config.dat` corresponent. Exemples:

### Fleck

./Fleck/fleck Fleck/config.dat

En aquest cas, `Fleck/config.dat` conté la configuració per al mòdul Fleck, i la carpeta `Fleck/Testing/` conté tots els fitxers que es poden distorsionar.

### Gotham

./Gotham/gotham Gotham/config.dat

### Gotham (versió mini)

./Gotham/gotham\_mini Gotham/config.dat

### Enigma

./Worker/Enigma/enigma Worker/Enigma/config.dat

### Harley

./Worker/Harley/harley Worker/Harley/config.dat

---

## Fitxers de Configuració (`config.dat`)

Cada mòdul té un fitxer `config.dat` al seu directori corresponent. Aquest fitxer inclou les dades necessàries per inicialitzar el mòdul (adreces IP, ports i altres paràmetres).

## Testing

Dins de la carpeta `Fleck/Testing/` es troben tots els fitxers que es poden utilitzar per provar les funcionalitats de distorsió:

* Fitxers `.txt` per a distorsió de text.
* Fitxers `.jpg` o `.png` per a imatges.
* Fitxers `.wav` o `.mp3` per a audio.

Aquests fitxers poden ser seleccionats per enviar-los a Gotham perquè siguin processats pels workers corresponents.
