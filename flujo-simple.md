# Flujo TRES65 — Agente María

```mermaid
flowchart TD
    A([Cliente escribe]) --> B[María saluda\npide nombre completo]
    B --> C[[Botones\nPara vivir · Para invertir · Algo más]]

    C -->|Para vivir| D[[Botones\nComprar · Rentar]]
    C -->|Para invertir| E[[Lista\nUso comercial · Renta habitacional]]
    C -->|Algo más| D

    D --> F[[Lista de presupuesto]]
    E --> F

    F --> G[Pregunta ciudad\n¿Ya estás en Mérida?]
    G --> H[[Lista de tipo de propiedad]]
    H --> I[Pregunta características\nalberca · recámaras · zona · etc.]
    I --> J[Pide correo]
    J --> K[María redacta la ficha]

    K --> L[[Botones\nTodo correcto · Algo está mal]]
    L -->|Algo está mal| K
    L -->|Todo correcto| M[EasyBroker:\nbusca propiedades disponibles]

    M --> N[Envía ficha al CRM\nMuestra propiedades encontradas]
    N --> O[[Botones\nAgendar llamada · Por WhatsApp]]

    O -->|Agendar llamada| P([Link Calendly])
    O -->|Por WhatsApp| Q([Tarjeta de contacto\ndel asesor])

    style A fill:#25D366,color:#fff
    style P fill:#0084FF,color:#fff
    style Q fill:#0084FF,color:#fff
```

---

**Entradas al flujo**

| | Diferencia |
|--|--|
| 🔗 Link directo | Saludo estándar |
| 📢 Anuncio Meta | Etiqueta automática del anuncio |
| 🏠 Anuncio de propiedad | Saludo y foto específicos |
| 📋 Formulario Lead Ad | Datos pre-llenados, salta al paso 2 |

**Follow-ups automáticos**

- **4 horas** sin respuesta → botones de retomada
- **23 horas** sin respuesta → plantilla aprobada por Meta
