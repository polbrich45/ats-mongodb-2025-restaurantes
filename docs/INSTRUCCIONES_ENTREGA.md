# Instrucciones para la Entrega de la Práctica MongoDB

## Flujo de Trabajo con GitHub

### 1. Fork del Repositorio

- Haz un fork de este repositorio a tu cuenta personal de GitHub.
- Clona tu fork a tu máquina local:
  ```bash
  git clone https://github.com/albertgilopez/ats-mongodb-2025-restaurantes.git
  cd ats-mongodb-2025-restaurantes
  ```

### 2. Estructura de tu Entrega

Deberás mantener la siguiente estructura en tu repositorio:

```plaintext
ats-mongodb-2025-restaurantes/
├── datasets/
│   ├── restaurants.json
│   └── inspections.json
├── scripts/
│   └── consultas.js
├── docs/
│   ├── ENUNCIADO.md
│   ├── GUIA_PRACTICA.md
│   ├── INSTRUCCIONES_ENTREGA.md
└── README.md
```

### 3. Desarrollo de la Práctica

1. **Análisis y Diseño**:
   - Estudia los datasets proporcionados.
   - Diseña el esquema de la base de datos.
   - Documenta tus decisiones de diseño.

2. **Implementación**:
   - Importa los datos a MongoDB.
   - Implementa las consultas requeridas.
   - Optimiza el rendimiento con índices.

3. **Documentación**:
   - Crea un informe detallado en `informe/README.md`.
   - Incluye capturas de pantalla de los resultados.
   - Justifica todas tus decisiones técnicas.

### 4. Entrega Final

- Asegúrate de que tu repositorio contiene todos los archivos necesarios.
- Crea un Pull Request desde tu fork al repositorio original.
- En la descripción del PR, incluye:
  - Tu nombre completo.
  - Grupo de prácticas.
  - Breve resumen de tu enfoque.
- Además, sube un archivo **.ZIP** con tu informe en PDF y la presentación en el Campus Virtual.

## Criterios de Evaluación

- **Correcta implementación de las consultas** (30%)
- **Calidad del análisis y justificación técnica** (30%)
- **Documentación clara y completa** (10%)
- **Presentación / Exposición en clase** (25%)
- **Uso adecuado de Git/GitHub** (5%)

## Fecha Límite

La fecha límite para crear el Pull Request es el **20/03/2025 a las 23:59**.

## Preguntas Frecuentes

**P: ¿Puedo modificar los datasets originales?**  
R: No, debes trabajar con los datasets proporcionados sin modificarlos. Sí que es posible combinarlos y crear nuevas colecciones, si crees que puede ser relevante para realizar las consultas o por decisión de diseño.

**P: ¿Puedo utilizar herramientas adicionales?**  
R: Sí, puedes utilizar cualquier herramienta que consideres útil, pero debes documentar su uso. También estan permitidas herramientas de generación de código del estilo ChatGPT. Si las utilizas documenta su uso y escribe los prompts que has utilizado para llegar a esos resultados.

**P: ¿Cómo debo documentar mis consultas?**  
R: Para cada consulta, incluye:
- El código de la consulta.
- Una explicación de su propósito.
- Una captura de pantalla del resultado.
- Análisis del rendimiento (si aplica).

**P: ¿Es obligatorio realizar la tarea opcional avanzada?**  
R: No, la tarea opcional avanzada es voluntaria, pero si la realizas y está correctamente implementada, puedes obtener hasta 2 puntos extra en la evaluación. Si te planteas realizarla y vas a utilizar el framework de LangChain solicita la API Key de OpenAI para utilizar los LLMs.

**P: ¿Cómo se evaluará la calidad del informe?**  
R: Se espera que el informe sea claro y estructurado, con explicaciones detalladas, capturas de pantalla relevantes y justificación de todas las decisiones técnicas tomadas. En el informe puedes hacer suposiciones, por ejemplo, si planteas este sistema como una aplicación de consulta para la administración, podrías suponer el tipo de consultas más frecuentes que realizarían los usuarios para justificar tus decisiones de diseño técnicas.

**P: ¿Puedo trabajar en pareja o en grupo?**  
R: La práctica debe realizarse en grupos de 2 con el compañero o compañera asignado en el grupo de inscripción del Campus Virtual. Sin embargo, también puedes debatir ideas y conceptos con tus compañeros.

**P: ¿Cómo debe ser la presentación final?**  
R: La presentación final debe durar un máximo de 10 minutos y cubrir los siguientes puntos:
- Presentación del caso de uso o prueba de concepto planteada.
- Explicación breve del esquema de la base de datos y decisiones de diseño.
- Ejemplos clave de consultas implementadas y su resultado.
- Explicación de mejoras de rendimiento y optimización.
- Reflexión final sobre dificultades encontradas y aprendizajes adquiridos.

**P: ¿Qué formato debe tener la presentación?**  
R: Puedes usar cualquier herramienta para realizar la presentación (PowerPoint, Google Slides, PDF, Notion, etc.), pero debe ser clara y concisa. Se recomienda incluir capturas de pantalla y gráficos para ilustrar los puntos clave y evitar diapositivas con demasiado texto. Recuerda que la presentación és prácticamente 1/3 de la nota final de las prácticas.




