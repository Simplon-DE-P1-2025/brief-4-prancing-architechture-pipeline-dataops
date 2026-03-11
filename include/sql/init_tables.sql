-- ─────────────────────────────────────────────────────────────────────────────
-- Table principale : données valides Chicago Crimes
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.chicago_crimes (
    id                    BIGINT PRIMARY KEY,
    case_number           VARCHAR(20),
    date                  TIMESTAMP,
    block                 VARCHAR(100),
    iucr                  VARCHAR(10),
    primary_type          VARCHAR(100),
    description           VARCHAR(200),
    location_description  VARCHAR(100),
    arrest                BOOLEAN,
    domestic              BOOLEAN,
    beat                  VARCHAR(10),
    district              VARCHAR(10),
    ward                  VARCHAR(10),
    community_area        VARCHAR(10),
    fbi_code              VARCHAR(10),
    x_coordinate          DOUBLE PRECISION,
    y_coordinate          DOUBLE PRECISION,
    year                  INTEGER,
    updated_on            TIMESTAMP,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION
);

-- ─────────────────────────────────────────────────────────────────────────────
-- Table quarantaine : données avec coordonnées hors bornes Chicago
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.chicago_crimes_quarantine (
    id                    BIGINT,
    case_number           VARCHAR(20),
    date                  TIMESTAMP,
    block                 VARCHAR(100),
    iucr                  VARCHAR(10),
    primary_type          VARCHAR(100),
    description           VARCHAR(200),
    location_description  VARCHAR(100),
    arrest                BOOLEAN,
    domestic              BOOLEAN,
    beat                  VARCHAR(10),
    district              VARCHAR(10),
    ward                  VARCHAR(10),
    community_area        VARCHAR(10),
    fbi_code              VARCHAR(10),
    x_coordinate          DOUBLE PRECISION,
    y_coordinate          DOUBLE PRECISION,
    year                  INTEGER,
    updated_on            TIMESTAMP,
    latitude              DOUBLE PRECISION,
    longitude             DOUBLE PRECISION,
    quarantine_reason     VARCHAR(200),        -- raison de la mise en quarantaine
    quarantined_at        TIMESTAMP            -- date/heure du chargement en quarantaine
);
