# Laboratorio 2: Sistemas Distribuidos

## Integrantes

Alberto Oñate - 202173103-2



## Arquitectura de Despliegue y Red

VM1: Broker Central 
VM2: Productores + Nodo DB3 
VM3: Consumidores + Nodo DB2 
VM4:  Banco USM + Nodo DB1 


## Instrucciones de Ejecución

### Paso 1: Levantar el Broker Central (VM1)
Conéctate a la VM1, accede a la carpeta `vm1` y ejecuta:

cd vm1
sudo make run

### Paso 2: Levantar el Banco y el Nodo DB1 (VM4)
Conéctate a la VM4, accede a la carpeta `vm4` y ejecuta:

cd vm4
sudo make run

### Paso 3: Levantar los Consumidores y el Nodo DB2 (VM3)
Conéctate a la VM3, accede a la carpeta `vm3` y ejecuta:

cd vm3
sudo make run

### Paso 4: Levantar los Productores y el Nodo DB3 (VM2)
Conéctate a la VM2, accede a la carpeta `vm2` y ejecuta:

cd vm2
sudo make run



## Apagado del Sistema y Extracción del Reporte (`Reporte.txt`)

Debido a que `docker compose down` destruye el contenedor inmediatamente después de apagarlo, para recuperar el archivo en la máquina host (VM1) debes seguir estos pasos:

Detener el contenedor del Broker (en VM1) sin destruirlo:

    cd vm1
    sudo docker compose stop broker

Copiar el reporte del contenedor a tu directorio local de la VM1:**
    
    sudo docker cp broker:/app/Reporte.txt .

Destruir los contenedores para limpiar recursos en VM1:**
    
    sudo make stop

Una vez finalizada la simulación, podrás visualizar el reporte generado localmente en la VM1 ejecutando:

cat Reporte.txt
