#para el cifrado
#cifrado.py - Cifrado simétrico de los mensajes entre Central y las estaciones con Fernet,
#cifra con AES de 128 bits y añade un HMAC-SHA256 que comprueba que nadie ha cambiado el mensaje por el hash

import json
from cryptography.fernet import Fernet, InvalidToken


#Cifra un diccionario y devuelve texto (sin saltos de línea)
def cifrar(clave, datos):
    return Fernet(clave).encrypt(json.dumps(datos).encode("utf-8")).decode("utf-8")


#Descifra el texto y devuelve el diccionario, o None si la clave no es la
#buena o el mensaje se ha modificado por el camino
def descifrar(clave, texto):
    try:
        return json.loads(Fernet(clave).decrypt(texto.encode("utf-8")).decode("utf-8"))
    except (InvalidToken, ValueError, TypeError, AttributeError):
        return None
