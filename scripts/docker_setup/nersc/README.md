# PSANA Docker scripts 


## Building

Build a docker image for different versions of psana

### PSANA2 
```bash
./build_docker.sh
```

or
```bash
./build_docker.sh 2
```

### PSANA1 - Python2
```bash
./build_docker.sh 1-py2
```

### PSANA1 - Python3
```bash
./build_docker.sh 1-py3
```

### CrystFEL
```bash
./build_docker.sh 1-py2
./build_docker_crystfel.sh
```
