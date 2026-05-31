# Flujo de Conversación — Agente TRES65

Abre preview con `Cmd+Shift+V`

```mermaid
flowchart TD
    A([📱 Cliente escribe por WhatsApp]) --> B{Tipo de entrada}

    B -->|Link directo| C[Label: link-directo\nSaludo estándar de María]
    B -->|Anuncio Meta Ads| D[Label: ad-nombre-anuncio\nNota privada con datos del anuncio\nSaludo estándar de María]
    B -->|Anuncio de propiedad específica| E[Saludo personalizado de la propiedad\nFotos si hay imagen en el anuncio]
    B -->|Formulario Lead Ad| F[Datos pre-llenados: nombre, correo, teléfono\nSalto directo a paso 2]

    C --> G
    D --> G
    E --> G
    F --> H

    G[María pide nombre completo]
    G --> G1{Cliente da nombre?}
    G1 -->|Solo primer nombre| G2[María pide apellido]
    G2 --> H
    G1 -->|Nombre completo| H

    H[Actualiza contacto en Chatwoot\nPrograma follow-up 4h]

    H --> I{Ya tiene intención?}
    I -->|No| I1[[Botones: Para vivir / Para invertir / Algo más]]
    I -->|Sí| J

    I1 -->|Para vivir| J
    I1 -->|Para invertir| INV
    I1 -->|Algo más| EXP

    J{Ya tiene tipo?}
    J -->|No| J1[[Botones: Comprar / Rentar]]
    J -->|Sí| K
    J1 --> K

    K[[Lista de presupuesto]]
    K --> L

    L{Busca vivir?}
    L -->|Sí| L1[María pregunta ciudad:\n'ya estás en Mérida o de dónde te mudas?']
    L -->|No / Invertir| M
    L1 --> M

    M[[Lista de tipo de propiedad\nCasa / Depto / Terreno / etc.]]
    M --> N

    N[María pregunta características:\nalberca, recámaras, jardín, etc.]
    N --> N1{Menciona características\nque coinciden con EasyBroker?}
    N1 -->|Sí| N2[Mensaje: 'tengo opciones disponibles\ncon esas características'\nContinúa flujo]
    N1 -->|No| O
    N2 --> O

    O[María pide correo]
    O --> O1{Cliente da correo?}
    O1 -->|Dice que no tiene| O2[Correo: Por definir\nContinúa]
    O1 -->|Da correo| O2

    O2 --> P[María redacta la ficha completa\n+ token CONFIRMAR_FICHA]

    P --> Q[[Botones: Todo correcto / Algo está mal]]

    Q -->|Algo está mal| Q1[María pide corrección\nRegresa a generar ficha]
    Q1 --> P

    Q -->|Todo correcto| R[Mensaje: 'déjame revisar opciones...'\nBúsqueda en EasyBroker]

    R --> R1{Hay propiedades?}
    R1 -->|Sí| R2[Envía títulos y precios\nde propiedades encontradas]
    R1 -->|No| S
    R2 --> S

    S[Envía ficha a Zapier → CRM\nMarca como calificado en Chatwoot]

    S --> T[[Botones de contacto:\nAgendar llamada / Por WhatsApp]]

    T -->|Agendar llamada| T1[Envía link de Calendly]
    T -->|Por WhatsApp| T2{Ya sabe el tema?}
    T2 -->|No| T3[Pregunta: '¿de qué quieres hablar\ncon el asesor?']
    T3 --> T4
    T2 -->|Sí| T4[Envía tarjeta de contacto del asesor]

    T4 --> U([✅ Conversación completada\nAsesor toma el control])

    %% FLUJO INVERSIÓN
    INV[Flujo Inversión]
    INV --> INV1[[Lista: Uso comercial / Renta habitacional]]
    INV1 -->|Renta habitacional| INV2[[Botones: Largo plazo / Corto plazo / Airbnb]]
    INV1 -->|Uso comercial| INV3[[Lista de tipo de propiedad]]
    INV2 --> INV3
    INV3 --> INV4[[Lista de presupuesto]]
    INV4 --> INV5{Conoce Mérida?}
    INV5 -->|No| INV6[María orienta sobre zonas]
    INV5 -->|Sí| O
    INV6 --> O

    %% FLUJO EXPLORATORIO
    EXP[Flujo Exploratorio\nAlgo más]
    EXP --> EXP1[María pregunta una cosa a la vez:\ntipo de necesidad, tiempo, presupuesto]
    EXP1 --> O

    %% FOLLOW-UPS
    H -.->|Sin respuesta 4h| FU1[[Follow-up: Botones\nCatálogo / No listo / Hablar asesor]]
    FU1 -.->|Sin respuesta 23h| FU2[[Plantilla WhatsApp aprobada por Meta]]

    %% FILTROS
    A --> SPAM{Filtros de entrada}
    SPAM -->|Número ya bloqueado| BLOCK([🚫 Ignorado permanentemente])
    SPAM -->|SEXUAL / INSULT| BLOCK2([🚫 Bloqueado + label rojo spam\nBorrado a medianoche])
    SPAM -->|ROMANTIC| ROM[María redirige\nSi reincide → bloqueado]
    SPAM -->|PERSONAL_QUESTION| PERS[María redirige\nSigue el flujo normal]
    SPAM -->|Proveedor / Reclutador| PROV[Mensaje de proveedor\nConversación cerrada en Chatwoot]

    %% AGENTE HUMANO
    T4 -.->|Asesor asigna conversación en Chatwoot| AG[Bot pausado\nAsesor atiende directamente]
```

---

## Entradas al flujo

| Tipo | Cómo llega | Diferencia |
|------|-----------|------------|
| **Link directo** | Comparte el link de WhatsApp | Label `link-directo`, saludo estándar |
| **Anuncio Meta** | Pica en ad de Facebook/Instagram | Label `ad-{nombre}`, nota con datos del anuncio |
| **Propiedad específica** | Anuncio de una propiedad configurada | Saludo y foto de esa propiedad |
| **Lead Ad** | Llena formulario en el anuncio | Datos pre-llenados, salta directamente al paso 2 |

## Tokens que controlan el flujo

| Token | Qué hace |
|-------|---------|
| `CONFIRMAR_FICHA` | Manda botones de confirmación de ficha |
| `MANDAR_BOTONES_COMPRAR_RENTAR` | Manda botones Comprar / Rentar |
| `MANDAR_BOTONES_VIVIR_INVERTIR` | Manda botones Para vivir / Para invertir / Algo más |
| `MANDAR_BOTONES_CONTACTO` | Manda botones de contacto con asesor |
| `PREGUNTAR_TEMA_ASESOR` | Pide al cliente el tema antes de conectar |
