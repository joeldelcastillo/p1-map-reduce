## Map Reduce



### Problema:

Se desea implementar un programa que permita la lectura de un fichero (Word, texto) y devuelva el
índice de palabras empleadas en el documento (la estructura del índice tiene que ser en el formato
<palabra, frecuencia>).
Nota: La elección del documento es de libre albedrío con un peso mínimo de un 1Tb. Se sugiere que sea
en idioma inglés y que sea un texto coherente (libros, historias, dramas, novelas, etc). Para lograr el peso
requerido del documento, se puede hacer varios append de la misma información al final del documento
original y repetir este proceso varias veces hasta lograr la meta. Por ejemplo: documento final =
documento final + documento original - (docF = docF+docO).

### Requisitos:



| **Requisito**                                                | **Status**  |
| ------------------------------------------------------------ | ----------- |
| Es obligatorio el uso de la filosofía MapReduce, en este caso sobre una arquitectura de nodo simple (una PC con varios cores), pero, paralelizable (usar hilos de programación) y distribuido (diferentes pools de hilos). | En progreso |
| El documento debe ser dividido en ficheros (chunks) de hasta 20mb. | En progreso |
| La cantidad de nodos puede ser aleatoria (dependiendo de las prestaciones de sus computadoras), pero, con un mínimo de 2 nodos reduce y 4 nodos map (2 por cada reduce). | En progreso |
| Almacenar en ficheros texto, la salida de cada paso de la técnica MapReduce. | En progreso |
| Garantizar la opción de fallo sobre los distintos nodos en tiempo de ejecución de la técnica MapReduce. | En progreso |
| Implementar un esquema exitoso de nodo coordinator, que sea capaz de asignar tareas y controlar el estado de los nodos map y reduce. | En progreso |
| si mínimamente tienen 12 chunks del fichero de entrada y 4 nodos map, significa que cada nodo podrá procesar más de un chunk y esta labor debe ser controlada y coordinada por el nodo coordinator). | En progreso |
|                                                              |             |

## Autores

\- [Joel del Castillo](https://www.github.com/joeldelcastillo)

\- [Pamela Mena]()

\- [Paúl Quimbita]()

\- [José Santillán]()
