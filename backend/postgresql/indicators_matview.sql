    DROP MATERIALIZED VIEW IF EXISTS indicators_matview CASCADE;
    

    -- Materialized View pour les indicateurs ASC
    DO $$
    DECLARE
        -- EVENTS
        events_bool_cols TEXT[] := ARRAY[
            'uaMxLFFBpO4','fKg4j7LyDvx','TA3vzccc5bL','urpAtvlPq9c','KgGKMku690L','ZLApDELSxee',
            'wVaK6DCgouW','OeUsVjtu6xU','jAmm3DLrXRA','sV01jFwrJzb','obXiOVy1W8E','jvAwC2yN080',
            'W5yUnYU2aXk','WSmH5J3vW6j','rUyAVcnsjR1','ld9SceWeuaw','PEo9RiQ3vnt','d6r9ZbTIxMd','oU7OlvYyq9m',
            'WNSaYn90Sh4','DrPiZYvBKj1',
            'o5ekXKzI6QG','RFXrTPSVOce','IByG3B7EKq6','we67w0fFoas','D63bI4wkZvL','LVEhpIZQPlc',
            'd3njr4pj5Np','HV4SzqtMkDZ','Daj1R7CbEEv','oV9wAqcGQLh','DzoAFCqUpEV','L0uyCnLsw7U',
            'HFaA8rTyEjo','nVoHOk96mki','HNqYMoPV5FX','sxXW0hhf3B0','eHcZTpdz5Jm','krGPBkPoypn',
            'VUw7r4npSPy','deleted'
        ];
        events_text_cols TEXT[] := ARRAY[
            'Qdy5VdNU6yb','Lw3FqC66H8f','JvpHtpl24Iw','o1ImyFA4GN5','YlFqhP3b6up','yZx9U6ExftO',
            'aJP29JC9lRG','EJ4cukfCCNI','XTVAi3UbimG','pEmu10xfoBu','LAFrQ2XGw6c','AgnRPmeEvcX',
            'hfrgBauAXHH','fv3D2nuPL54','WbgApzYRexi','ZSIPhlUprHh'
        ];
        events_int_cols TEXT[] := ARRAY[
            'bTBv9y0xr2c','AqJgPz8KxFX','J0vbT2fjVpi'
        ];

        -- ATTRIBUTES
        attributes_bool_cols TEXT[] := ARRAY[
            'wfwjAVT0gVA','nalzdP5XUMZ'
        ];
        attributes_text_cols TEXT[] := ARRAY[
            'K9wH3KPlohz','UrdrExJmPcF','EimePKtHtUd','sYY4FnnQLAK','hUl1tJSC0pV','g5AD18Pyx7g','pIYIdGYDiZA',
            'YzTZQW5xRY3','w7XqUyjpZr9','gMNsFaScKGc','lpAqrE3CvLZ','HZm1SrVlxN5','S7OKlFbZsDd','xLqd2FibwYZ',
            'STUQ4XtZ805','l1JVuc7BP1W','UlLFqronELf','xUq6Qsq4eQb','xOMccRImFH0','vVxCyx21Ywy','LCYJJUmSJ2o',
            'KVgxSnZiw38','idjYeYouHx6','sydZ9u6mnuS','iIyc0er2dHe','RnIPyB5WFgC','kxuii0VEL0o','atRm1zUCzR8',
            'fa4IC3iimcp','R2jYJ4haQ4i','AE3ovTzZPE8','As79zeozkyx'
        ];

        col TEXT;
    BEGIN
        -- Colonnes booléennes (création + conversion si existante)
        FOREACH col IN ARRAY events_bool_cols LOOP
            EXECUTE format('ALTER TABLE events ADD COLUMN IF NOT EXISTS %I TEXT', col);
            -- Conversion robuste TEXT -> BOOLEAN
            EXECUTE format(
                'ALTER TABLE events ALTER COLUMN %I TYPE boolean 
                USING CASE 
                        WHEN lower(%I::text) IN (''true'',''yes'',''ok'') THEN true
                        WHEN lower(%I::text) IN (''false'',''no'') THEN false
                        ELSE false
                    END',
                col, col, col
            );
            -- Valeur par défaut pour les nouvelles lignes
            EXECUTE format('ALTER TABLE events ALTER COLUMN %I SET DEFAULT false', col);
        END LOOP;
        -- Colonnes texte
        FOREACH col IN ARRAY events_text_cols LOOP
            EXECUTE format('ALTER TABLE events ADD COLUMN IF NOT EXISTS %I TEXT DEFAULT NULL', col);
        END LOOP;
        -- Colonnes bigint
        FOREACH col IN ARRAY events_int_cols LOOP
            EXECUTE format('ALTER TABLE events ADD COLUMN IF NOT EXISTS %I BIGINT DEFAULT NULL', col);
        END LOOP;
        -- ALTER TABLE events ALTER COLUMN event_date TYPE DATE USING to_date(event_date,'YYYY-MM-DD');

        -- #####################################################################   

        FOREACH col IN ARRAY attributes_bool_cols LOOP
            EXECUTE format('ALTER TABLE attributes ADD COLUMN IF NOT EXISTS %I TEXT', col);
            -- Conversion robuste TEXT -> BOOLEAN
            EXECUTE format(
                'ALTER TABLE attributes ALTER COLUMN %I TYPE boolean 
                USING CASE 
                        WHEN lower(%I::text) IN (''true'',''yes'',''ok'') THEN true
                        WHEN lower(%I::text) IN (''false'',''no'') THEN false
                        ELSE false
                    END',
                col, col, col
            );
            -- Valeur par défaut pour les nouvelles lignes
            EXECUTE format('ALTER TABLE attributes ALTER COLUMN %I SET DEFAULT false', col);
        END LOOP;


        FOREACH col IN ARRAY attributes_text_cols LOOP
            EXECUTE format('ALTER TABLE attributes ADD COLUMN IF NOT EXISTS %I TEXT DEFAULT NULL', col);
        END LOOP;

        -- ALTER TABLE enrollments ALTER COLUMN enrollment_date TYPE DATE USING to_date(enrollment_date,'YYYY-MM-DD');
    END $$;


    CREATE OR REPLACE FUNCTION get_age_group(event_year INT, birth_year INT)
        RETURNS TEXT AS $$
        DECLARE
            age INT;
        BEGIN
            IF event_year IS NULL OR birth_year IS NULL THEN
                RETURN NULL;
            END IF;

            age := event_year - birth_year;

            IF age >= 18 AND age < 30 THEN RETURN '18-29';
            ELSIF age >= 30 AND age < 45 THEN RETURN '30-44';
            ELSIF age >= 45 AND age < 60 THEN RETURN '45-59';
            ELSIF age >= 60 AND age <= 75 THEN RETURN '60-75';
            ELSIF age > 75 THEN RETURN '75+';
            ELSE 
                RETURN NULL;
            END IF;
        END;
        $$ LANGUAGE plpgsql IMMUTABLE;



    -- ATTRIBUTES
    CREATE INDEX IF NOT EXISTS idx_attributes_program ON attributes(program);
    CREATE INDEX IF NOT EXISTS idx_attributes_deleted ON attributes(deleted);
    CREATE INDEX IF NOT EXISTS idx_attributes_enrollment_id ON attributes(enrollment_id);
    CREATE INDEX IF NOT EXISTS idx_attributes_program_deleted ON attributes(program, deleted);
    CREATE INDEX IF NOT EXISTS idx_attributes_keys ON attributes(tei_id, orgunit_id, enrollment_id);
    CREATE INDEX IF NOT EXISTS idx_attributes_tei_id_orgunit_id_program ON attributes(tei_id, orgunit_id, program);
    CREATE INDEX IF NOT EXISTS idx_attr_program_deleted_tei_org_enr ON attributes(program, deleted, tei_id, orgunit_id, enrollment_id);
    -- EVENTS
    CREATE INDEX IF NOT EXISTS idx_events_deleted ON events(deleted);
    CREATE INDEX IF NOT EXISTS idx_events_event_date ON events(event_date);
    CREATE INDEX IF NOT EXISTS idx_events_program_stage ON events(program_stage_id);
    CREATE INDEX IF NOT EXISTS idx_events_program_deleted ON events(program, deleted);
    CREATE INDEX IF NOT EXISTS idx_events_tei_org_enr ON events(tei_id, orgunit_id, enrollment_id);
    CREATE INDEX IF NOT EXISTS idx_events_tei_program_stage ON events(tei_id, program, program_stage_id);
    CREATE INDEX IF NOT EXISTS idx_events_stage_deleted ON events(program_stage_id, deleted) WHERE deleted IS NOT TRUE;
    CREATE INDEX IF NOT EXISTS idx_events_tei_org_enr_eventdate ON events(tei_id, orgunit_id, enrollment_id, event_date) WHERE deleted IS NOT TRUE;
    CREATE INDEX IF NOT EXISTS idx_events_status_enrollmentstatus ON events(status, enrollment_status) WHERE deleted IS NOT TRUE;
    -- ENROLLMENTS
    CREATE INDEX IF NOT EXISTS idx_enrollments_deleted ON enrollments(deleted);
    CREATE INDEX IF NOT EXISTS idx_enrollments_keys ON enrollments(tei_id, orgunit_id);
    CREATE INDEX IF NOT EXISTS idx_enrollments_program_deleted ON enrollments(program, deleted);
    CREATE INDEX IF NOT EXISTS idx_enrollments_orgunit_program ON enrollments(orgunit_id, program);
    CREATE INDEX IF NOT EXISTS idx_enroll_program_deleted ON enrollments(program, deleted) WHERE deleted IS NOT TRUE;
    CREATE INDEX IF NOT EXISTS idx_enroll_tei_org ON enrollments(tei_id, orgunit_id, enrollment_date) WHERE deleted IS NOT TRUE;
    -- ORGUNITS (if frequently used in joins)
    CREATE INDEX IF NOT EXISTS idx_orgunit_id ON "organisationUnits"(id);


    CREATE MATERIALIZED VIEW indicators_matview AS
    
        -- For attributes_base
        WITH attributes_base AS (
            SELECT
                a.id,
                a.tei_id,
                a.enrollment_id,
                a.orgunit_id,
                a.program,
                -- a."displayName",
                (a.created::timestamp)::DATE AS created_at,
                NULLIF(a."K9wH3KPlohz",'')::int AS age, -- Age
                COALESCE(NULLIF(a."UrdrExJmPcF",''),'M') AS sex, -- Sexe (M/F)
                COALESCE(NULLIF(a."EimePKtHtUd",''),'ASC') AS status, -- Statut (ASC/RC)
                COALESCE(NULLIF(a."sYY4FnnQLAK",''),'Vivant') AS statut_vie, -- Statut d'Existance
                COALESCE(NULLIF(a."hUl1tJSC0pV",'')::int,0)::int AS birth_year, -- Année de naissance
                NULLIF(a."g5AD18Pyx7g",'') AS year_work_start, -- Année debut Travail
                NULLIF(a."pIYIdGYDiZA",'') AS village, -- Village ASC/RC
                NULLIF(a."YzTZQW5xRY3",'') AS situation_matrimoniale, -- Situation Matrimoniale
                NULLIF(a."w7XqUyjpZr9",'') AS niveau_instruction, -- Niveau Instruction
                NULLIF(a."gMNsFaScKGc",'') AS occupation, -- Occupation
                -- Booléens ASC/RC déjà convertis
                (lower(a."wfwjAVT0gVA"::text) IN ('true','1','yes','ok','vrai','oui'))::BOOLEAN AS proposition, -- ASC/RC Proposé
                (lower(a."nalzdP5XUMZ"::text) IN ('true','1','yes','ok','vrai','oui'))::BOOLEAN AS valid_proposition, -- ASC/RC Proposition Validée
                -- Dates
                NULLIF(a."lpAqrE3CvLZ",'')::DATE AS birth_date, -- Date de naissance
                NULLIF(a."HZm1SrVlxN5",'')::DATE AS date_work_start, -- Date Début Travail
                NULLIF(a."S7OKlFbZsDd",'')::DATE AS death_date, -- Date décès
                NULLIF(a."xLqd2FibwYZ",'')::DATE AS accreditation_date, -- Date Accréditation
                NULLIF(a."STUQ4XtZ805",'')::DATE AS date_proposition, -- ASC/RC Date Proposition
                NULLIF(a."l1JVuc7BP1W",'')::DATE AS date_valid_proposition -- ASC/RC Date Proposition Validée
                -- NULLIF(a."UlLFqronELf",'') AS to_set_after, -- Autre Occupation
                -- NULLIF(a."xUq6Qsq4eQb",'') AS to_set_after, -- Autre Type Relais
                -- NULLIF(a."xOMccRImFH0",'') AS to_set_after, -- Cause décès
                -- NULLIF(a."vVxCyx21Ywy",'') AS to_set_after, -- Identifiant ASC/RC
                -- NULLIF(a."LCYJJUmSJ2o",'') AS to_set_after, -- Identifiant System
                -- NULLIF(a."KVgxSnZiw38",'') AS to_set_after, -- Identifiant Unique
                -- NULLIF(a."idjYeYouHx6",'') AS to_set_after, -- Lieu décès
                -- NULLIF(a."sydZ9u6mnuS",'') AS to_set_after, -- Localisation
                -- NULLIF(a."iIyc0er2dHe",'') AS to_set_after, -- Nom
                -- NULLIF(a."RnIPyB5WFgC",'') AS to_set_after, -- Numéro Pièce d'Identité
                -- NULLIF(a."kxuii0VEL0o",'') AS to_set_after, -- Photo
                -- NULLIF(a."atRm1zUCzR8",'') AS to_set_after, -- Prenom
                -- NULLIF(a."fa4IC3iimcp",'') AS to_set_after, -- Type Pièce d'Identité
                -- NULLIF(a."R2jYJ4haQ4i",'') AS to_set_after, -- Type Relais
                -- NULLIF(a."AE3ovTzZPE8",'') AS to_set_after, -- Téléphone 1 (Moov)
                -- NULLIF(a."As79zeozkyx",'') AS to_set_after, -- Téléphone 2
            FROM attributes a
            WHERE a.deleted IS NOT TRUE AND a.program='DdjHMnKg3wx'
        ),

        -- events_last_dates
        events_last_dates AS (
            SELECT tei_id, program, program_stage_id, MAX(event_date) AS last_event_date
            FROM events
            WHERE deleted IS NOT TRUE
            GROUP BY tei_id, program, program_stage_id
        ),

        -- For events_base
        events_base AS (
            SELECT
                e.id,
                e.tei_id,
                e.program,
                e.orgunit_id,
                e.enrollment_id,
                to_date(NULLIF(e.event_date::text,''),'YYYY-MM-DD') AS event_date,
                -- to_date(NULLIF(e.due_date,''),'YYYY-MM-DD') AS due_date,
                TO_CHAR(to_date(NULLIF(e.event_date::text,''),'YYYY-MM-DD'), 'YYYYMM')::text  AS event_period,
                EXTRACT(YEAR FROM to_date(NULLIF(e.event_date::text,''),'YYYY-MM-DD'))::int AS event_year,
                get_age_group(EXTRACT(YEAR FROM to_date(NULLIF(e.event_date::text,''),'YYYY-MM-DD'))::int, abb.birth_year) AS event_age_group,
                COALESCE(e.status='ACTIVE', FALSE) AS is_active_event,
                COALESCE(e.enrollment_status='ACTIVE', FALSE) AS is_active_enrollment,
                COALESCE(NULLIF(TRIM(e."bTBv9y0xr2c"), ''), '0')::int AS nbr_habitants_couvert,  -- Nombre d'habitants couverts
                COALESCE(NULLIF(TRIM(e."AqJgPz8KxFX"), ''), '0')::int AS nombre_menages_couverts,  -- Nombre de ménages couverts
                COALESCE(NULLIF(TRIM(e."J0vbT2fjVpi"), ''), '0')::int AS distance_usp_localite_km,  -- Distance USP - localité ( Km )
                --INFORMATIONS GENERALES -- program_stage_id='uYjFHpdIZ1n'    -- A Renseigner
                (e."D63bI4wkZvL" IS TRUE)::BOOLEAN AS existence_obstacle,  -- Existence d'un obstacle    -->  TEXT
                (CASE WHEN e."LVEhpIZQPlc" IS TRUE AND e."Lw3FqC66H8f" IN ('BON ETAT','MAUVAIS ETAT') THEN e."Lw3FqC66H8f" END)::TEXT AS affiches,             -- Affiches    -->  Etat Affiches
                (CASE WHEN e."d3njr4pj5Np" IS TRUE AND e."JvpHtpl24Iw" IN ('BON ETAT','MAUVAIS ETAT') THEN e."JvpHtpl24Iw" END)::TEXT AS boites_a_images,      -- Boites à images    -->  Etat Boites à images
                (CASE WHEN e."HV4SzqtMkDZ" IS TRUE AND e."o1ImyFA4GN5" IN ('BON ETAT','MAUVAIS ETAT') THEN e."o1ImyFA4GN5" END)::TEXT AS bottes,               -- Bottes    -->  Etat Bottes
                (CASE WHEN e."Daj1R7CbEEv" IS TRUE AND e."YlFqhP3b6up" IN ('BON ETAT','MAUVAIS ETAT') THEN e."YlFqhP3b6up" END)::TEXT AS caisse,               -- Caisse    -->  Etat Caisse
                (CASE WHEN e."oV9wAqcGQLh" IS TRUE AND e."yZx9U6ExftO"::TEXT IN ('BON ETAT','MAUVAIS ETAT') THEN e."yZx9U6ExftO"::TEXT END)::TEXT AS impermeables_raglan,  -- Imperméables/Raglan    -->  Etat Imperméables/Raglan
                (CASE WHEN e."DzoAFCqUpEV" IS TRUE AND e."aJP29JC9lRG" IN ('BON ETAT','MAUVAIS ETAT') THEN e."aJP29JC9lRG" END)::TEXT AS sac,                  -- Sac    -->  Etat Sac
                (CASE WHEN e."L0uyCnLsw7U" IS TRUE AND e."EJ4cukfCCNI" IN ('BON ETAT','MAUVAIS ETAT') THEN e."EJ4cukfCCNI" END)::TEXT AS smartphone,           -- Smartphone    -->  Etat Smartphone
                (CASE WHEN e."HFaA8rTyEjo" IS TRUE AND e."XTVAi3UbimG" IN ('BON ETAT','MAUVAIS ETAT') THEN e."XTVAi3UbimG" END)::TEXT AS stylos,               -- Stylos    -->  Etat Stylos
                (CASE WHEN e."nVoHOk96mki" IS TRUE AND e."pEmu10xfoBu" IN ('BON ETAT','MAUVAIS ETAT') THEN e."pEmu10xfoBu" END)::TEXT AS thermometre,          -- Thermomètre    -->  Etat Thermomètre
                (CASE WHEN e."HNqYMoPV5FX" IS TRUE AND e."LAFrQ2XGw6c" IN ('BON ETAT','MAUVAIS ETAT') THEN e."LAFrQ2XGw6c" END)::TEXT AS torche,               -- Torche    -->  Etat Torche
                (CASE WHEN e."sxXW0hhf3B0" IS TRUE AND e."AgnRPmeEvcX" IN ('BON ETAT','MAUVAIS ETAT') THEN e."AgnRPmeEvcX" END)::TEXT AS velo,                 -- Vélo    -->  Etat Vélo
                (CASE WHEN e."eHcZTpdz5Jm" IS TRUE AND e."hfrgBauAXHH" IN ('BON ETAT','MAUVAIS ETAT') THEN e."hfrgBauAXHH" END)::TEXT AS powerbank,            -- Powerbank    -->  Etat Powerbank
                (CASE WHEN e."krGPBkPoypn" IS TRUE AND e."fv3D2nuPL54" IN ('BON ETAT','MAUVAIS ETAT') THEN e."fv3D2nuPL54" END)::TEXT AS autre_equipement_one, -- Autre Equipement 1    -->  Etat Autre Equipement 1
                (CASE WHEN e."VUw7r4npSPy" IS TRUE AND e."WbgApzYRexi" IN ('BON ETAT','MAUVAIS ETAT') THEN e."WbgApzYRexi" END)::TEXT AS autre_equipement_two, -- Autre Equipement 2    -->  Etat Autre Equipement 2
                -- FORMATIONS  --  program_stage_id='vYuCO9VE4VO'
                (e."uaMxLFFBpO4" IS TRUE)::BOOLEAN AS formation_gestion_meg,    -- Formation Gestion Des Médicaments 
                (e."fKg4j7LyDvx" IS TRUE)::BOOLEAN AS formation_pecimne,        -- Formation PCIMNE-c (Paludisme+Toux/Rhume/Pneumonie+Diarrhée+Malnutrition) 
                (e."TA3vzccc5bL" IS TRUE)::BOOLEAN AS formation_assainissement, -- Formation en Assainissement Total Piloté par la Communauté
                (e."urpAtvlPq9c" IS TRUE)::BOOLEAN AS formation_change_cptm,    -- Formation en Communication pour le Changement de Comportement 
                (e."KgGKMku690L" IS TRUE)::BOOLEAN AS formation_comm_vih,       -- Formation en Communication sur le VIH/SIDA 
                (e."ZLApDELSxee" IS TRUE)::BOOLEAN AS formation_pf_comm,        -- Formation en PF-Communautaire 
                (e."wVaK6DCgouW" IS TRUE)::BOOLEAN AS formation_pec_pvvih,      -- Formation en Prise en Charge Psychosociale des PVVIH 
                (e."OeUsVjtu6xU" IS TRUE)::BOOLEAN AS formation_promotion,      -- Formation en Promotion des Pratiques Familiales Essentielles 
                (e."jAmm3DLrXRA" IS TRUE)::BOOLEAN AS formation_malnutrition,   -- Formation sur la Prise en Charge de la Malnutrition 
                (e."sV01jFwrJzb" IS TRUE)::BOOLEAN AS formation_paludisme,      -- Formation sur le Paludisme 
                (e."obXiOVy1W8E" IS TRUE)::BOOLEAN AS formation_coinfection_tb,    -- Formation en Co-Infection TB/VIH
                (e."jvAwC2yN080" IS TRUE)::BOOLEAN AS formation_maladi_non_transmissible,    -- Formation sur les Maladies Non Transmissibles
                (e."W5yUnYU2aXk" IS TRUE)::BOOLEAN AS formation_Maladi_tropicale,    -- Formation sur les Maladies Tropicales Négligées
                (e."WSmH5J3vW6j" IS TRUE)::BOOLEAN AS formation_suivi_rapportage,    -- Formation Monitorage Suivi Et Rapportage
                (e."rUyAVcnsjR1" IS TRUE)::BOOLEAN AS formation_qualite_soins_nc,    -- Formation en Qualité des Soins au Niveau Communautaire
                (e."ld9SceWeuaw" IS TRUE)::BOOLEAN AS formation_sante_mere,    -- Formation Santé de la Mère (Référence de la femme enceinte au cours des VAD)
                (e."PEo9RiQ3vnt" IS TRUE)::BOOLEAN AS formation_surveil_epidemiologique,    -- Formation sur la Surveillance Epidémiologique à Base Communautaire
                (e."d6r9ZbTIxMd" IS TRUE OR e."oU7OlvYyq9m" IS TRUE)::BOOLEAN AS formation_others,    -- Autre Formation 1 OU Autre Formation 2
                -- REUNION MENSUELLE  --  program_stage_id='HVs2gZcENZX'
                (e."WNSaYn90Sh4" IS TRUE)::BOOLEAN AS reunion_mensuelle, --> Participation Réunion Mensuelle 
                (e."DrPiZYvBKj1" IS TRUE OR e."we67w0fFoas" IS TRUE)::BOOLEAN AS supervision, --> Is Supervision Agent 
                (CASE 
                    WHEN (e."DrPiZYvBKj1" IS TRUE OR e."we67w0fFoas" IS TRUE) 
                    AND e."ZSIPhlUprHh" IN ('RFS','RM','ASC superviseur','Niveau District','Niveau Regions','Niveau Central') 
                    THEN e."ZSIPhlUprHh" 
                END)::TEXT AS supervision_person, --> Supervision Agent  DANS INFOS GENERAL
                (e."o5ekXKzI6QG" IS TRUE)::BOOLEAN AS suivi, --> Suivi Agent 
                (e."IByG3B7EKq6" IS TRUE)::BOOLEAN AS suivi_animateur_endogene, --> Suivi Animateur Endogene  DANS INFOS GENERAL
                (e."RFXrTPSVOce" IS TRUE)::BOOLEAN AS rapport_mensuel, --> Rapport Réunion Mensuelle 
                -- ETAT PERSONNE  --  program_stage_id='cOlFW2LHdud'
                (e."Qdy5VdNU6yb"='DEMISSION')::BOOLEAN AS demission, -- Raison Etat Personne | (ABANDON / DECES / DEMISSION / LICENCIEMENT / FAUTE GRAVE)
                (e."Qdy5VdNU6yb"='ABANDON')::BOOLEAN AS abandon, -- Raison Etat Personne | (ABANDON / DECES / DEMISSION / LICENCIEMENT / FAUTE GRAVE)
                (e."Qdy5VdNU6yb"='DECES')::BOOLEAN AS deces, -- Raison Etat Personne | (ABANDON / DECES / DEMISSION / LICENCIEMENT / FAUTE GRAVE)
                (e."Qdy5VdNU6yb"='LICENCIEMENT')::BOOLEAN AS licenciement, -- Raison Etat Personne | (ABANDON / DECES / DEMISSION / LICENCIEMENT / FAUTE GRAVE)
                (e."Qdy5VdNU6yb"='FAUTE GRAVE')::BOOLEAN AS faute_grave, -- Raison Etat Personne | (ABANDON / DECES / DEMISSION / LICENCIEMENT / FAUTE GRAVE)
                COALESCE(e."Qdy5VdNU6yb" NOT IN ('DEMISSION','ABANDON','DECES','LICENCIEMENT','FAUTE GRAVE'), TRUE)::BOOLEAN AS is_active,
                -- LATERAL JOIN FOR LAST DATES
                info.last_event_date AS last_info_event_date,
                form.last_event_date AS last_formation_event_date,
                reunion.last_event_date AS last_reunion_event_date,
                etat.last_event_date AS last_rapport_event_date
            FROM events e
                LEFT JOIN attributes_base abb ON abb.tei_id=e.tei_id AND abb.orgunit_id=e.orgunit_id AND abb.enrollment_id=e.enrollment_id AND abb.program=e.program
                LEFT JOIN events_last_dates info ON info.tei_id=e.tei_id AND info.program=e.program AND info.program_stage_id='uYjFHpdIZ1n'
                LEFT JOIN events_last_dates form ON form.tei_id=e.tei_id AND form.program=e.program AND form.program_stage_id='vYuCO9VE4VO'
                LEFT JOIN events_last_dates reunion ON reunion.tei_id=e.tei_id AND reunion.program=e.program AND reunion.program_stage_id='HVs2gZcENZX'
                LEFT JOIN events_last_dates etat ON etat.tei_id=e.tei_id AND etat.program=e.program AND etat.program_stage_id='cOlFW2LHdud'
        ),

        -- For enrollments_base
        enrollments_base AS (
            SELECT
                er.id,
                er.tei_id,
                er.orgunit_id,
                er.program,
                to_date(NULLIF(er.enrollment_date::text,''),'YYYY-MM-DD') AS enrollment_date,
                TO_CHAR(to_date(NULLIF(er.enrollment_date::text,''),'YYYY-MM-DD'),'YYYYMM') AS enrol_period,
                EXTRACT(YEAR FROM to_date(NULLIF(er.enrollment_date::text,''),'YYYY-MM-DD'))::int AS enrol_year,
                get_age_group(EXTRACT(YEAR FROM to_date(NULLIF(er.enrollment_date::text,''),'YYYY-MM-DD'))::int, aba.birth_year) AS enrol_age_group
                -- TO_CHAR(to_date(NULLIF(er.enrollment_date,''),'YYYY-MM-DD'),'MM') AS month,
                -- EXTRACT(YEAR FROM to_date(NULLIF(er.enrollment_date,''),'YYYY-MM-DD'))::int AS year,
                -- DATE_TRUNC('month', to_date(NULLIF(er.enrollment_date,''),'YYYY-MM-DD')) AS periode_mois,
            FROM enrollments er 
            LEFT JOIN attributes_base aba ON aba.tei_id=er.tei_id AND aba.orgunit_id=er.orgunit_id AND aba.program=er.program 
            WHERE er.deleted IS NOT TRUE
        ),

        -- events agrégés par enrollment + période + age_group (flags=OR des événements sur la période)
        events_agg AS (
            SELECT
                tei_id,
                enrollment_id,
                orgunit_id,
                event_period,
                event_age_group,
                MAX(event_date)                          AS last_event_date,
                MAX(last_reunion_event_date)             AS last_reunion_event_date,
                MAX(last_rapport_event_date)             AS last_rapport_event_date,
                BOOL_OR(reunion_mensuelle)               AS reunion_mensuelle,
                BOOL_OR(supervision)                     AS supervision,

                BOOL_OR(supervision_person='RFS')                     AS supervision_rfs,
                BOOL_OR(supervision_person='RM')                     AS supervision_rm,
                BOOL_OR(supervision_person='ASC superviseur')                     AS supervision_asc_superviseur,
                BOOL_OR(supervision_person='Niveau District')                     AS supervision_niveau_district,
                BOOL_OR(supervision_person='Niveau Regions')                     AS supervision_niveau_regions,
                BOOL_OR(supervision_person='Niveau Central')                     AS supervision_niveau_central,

                BOOL_OR(suivi)                           AS suivi,
                BOOL_OR(suivi_animateur_endogene)        AS suivi_animateur_endogene,
                BOOL_OR(rapport_mensuel)                 AS rapport_mensuel,
                BOOL_OR(deces)                           AS deces,
                -- équipements (on prend existence des états)
                BOOL_OR(sac='BON ETAT')                AS sac_good_state,
                BOOL_OR(velo='BON ETAT')               AS velo_good_state,
                BOOL_OR(stylos='BON ETAT')             AS stylos_good_state,
                BOOL_OR(torche='BON ETAT')             AS torche_good_state,
                BOOL_OR(bottes='BON ETAT')             AS bottes_good_state,
                BOOL_OR(caisse='BON ETAT')             AS caisse_good_state,
                BOOL_OR(affiches='BON ETAT')           AS affiches_good_state,
                BOOL_OR(powerbank='BON ETAT')          AS powerbank_good_state,
                BOOL_OR(smartphone='BON ETAT')         AS smartphone_good_state,
                BOOL_OR(thermometre='BON ETAT')        AS thermometre_good_state,
                BOOL_OR(boites_a_images='BON ETAT')    AS boites_a_images_good_state,
                BOOL_OR(impermeables_raglan='BON ETAT') AS impermeables_raglan_good_state,
                BOOL_OR(autre_equipement_one='BON ETAT') AS autre_equipement_one_good_state,
                BOOL_OR(autre_equipement_two='BON ETAT') AS autre_equipement_two_good_state,
                -- formations
                BOOL_OR(formation_gestion_meg)           AS formation_gestion_meg,
                BOOL_OR(formation_pecimne)               AS formation_pecimne,
                BOOL_OR(formation_assainissement)        AS formation_assainissement,
                BOOL_OR(formation_change_cptm)           AS formation_change_cptm,
                BOOL_OR(formation_comm_vih)              AS formation_comm_vih,
                BOOL_OR(formation_pf_comm)               AS formation_pf_comm,
                BOOL_OR(formation_pec_pvvih)             AS formation_pec_pvvih,
                BOOL_OR(formation_promotion)             AS formation_promotion,
                BOOL_OR(formation_malnutrition)          AS formation_malnutrition,
                BOOL_OR(formation_paludisme)             AS formation_paludisme,
                BOOL_OR(formation_coinfection_tb)        AS formation_coinfection_tb,
                BOOL_OR(formation_maladi_non_transmissible)        AS formation_maladi_non_transmissible,
                BOOL_OR(formation_Maladi_tropicale)        AS formation_Maladi_tropicale,
                BOOL_OR(formation_suivi_rapportage)        AS formation_suivi_rapportage,
                BOOL_OR(formation_qualite_soins_nc)        AS formation_qualite_soins_nc,
                BOOL_OR(formation_sante_mere)        AS formation_sante_mere,
                BOOL_OR(formation_surveil_epidemiologique)        AS formation_surveil_epidemiologique,
                BOOL_OR(formation_others)        AS formation_others,
                -- autres flags d'état
                BOOL_OR(demission)                       AS demission,
                BOOL_OR(abandon)                         AS abandon,
                BOOL_OR(licenciement)                    AS licenciement,
                BOOL_OR(faute_grave)                     AS faute_grave,
                BOOL_OR(is_active)                       AS is_active
            FROM events_base
            WHERE event_period IS NOT NULL
            GROUP BY tei_id, enrollment_id, orgunit_id, event_period, event_age_group
        ),

        -- Latest attributes per enrollment (une seule ligne par enrollment_id)
        attributes_latest AS (
            SELECT DISTINCT ON (enrollment_id) 
                id,
                tei_id,
                enrollment_id,
                orgunit_id,
                program,
                created_at,
                age,
                sex,
                status,
                statut_vie,
                birth_year,
                proposition,
                valid_proposition,
                birth_date,
                date_work_start,
                death_date,
                accreditation_date,
                date_proposition,
                date_valid_proposition,
                situation_matrimoniale,
                occupation
            FROM attributes_base
            WHERE program='DdjHMnKg3wx'
            ORDER BY enrollment_id, created_at DESC, id
        ),
        
        --- Period rows (unique orgunit × period × age_group)
        period_base AS (
            SELECT DISTINCT 
                program,
                orgunit_id, 
                COALESCE(event_period, enrol_period) AS period, 
                COALESCE(event_year::int, enrol_year::int)::int AS year, 
                COALESCE(event_age_group, enrol_age_group) AS age_group
            FROM (
                SELECT program, orgunit_id, event_period, event_year, event_age_group, NULL AS enrol_period, NULL AS enrol_year, NULL AS enrol_age_group
                FROM events_base
                WHERE event_period IS NOT NULL
                UNION ALL
                SELECT program, orgunit_id, enrol_period, enrol_year, enrol_age_group, NULL AS event_period, NULL AS event_year, NULL AS event_age_group
                FROM enrollments_base
                WHERE enrol_period IS NOT NULL
            ) t 
        )

        -- Full selection
        SELECT 
            -- Générer un ID unique à partir de colonnes existantes
            -- CONCAT(r.id,'_',p.period,'_',p.age_group,'_',a.sex,'_', a.status) AS uid,
            CONCAT(r.id,'_',p.period,'_',p.age_group,'_',a.sex,'_', a.status,'_', ROW_NUMBER() OVER(PARTITION BY r.id, p.period, p.age_group, a.sex, a.status ORDER BY r.id)) AS uid,
            r.id,
            r.tei_id,
            p.age_group,
            p.orgunit_id,
            p.period,
            a.sex,
            a.status,
            -- COALESCE(a.birth_year, NULL)::int AS birth_year,
            -- 0. Nombre total ASC/RC /* 0. Total ASC/RC */
            COUNT(DISTINCT r.id) FILTER (WHERE r.enrol_period=p.period  AND r.enrol_age_group=p.age_group) AS total,
            -- 2. Nombre ASC/RC actifs /* 1. ASC Actifs */
            COUNT(DISTINCT r.id) FILTER (WHERE a.statut_vie <> 'Décédé' AND v.is_active IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS actif,
            -- 3. ASC décédés /* 2. ASC décédés */
            COUNT(DISTINCT r.id) FILTER (WHERE 
                (a.statut_vie='Décédé' AND r.enrol_period=p.period AND r.enrol_age_group=p.age_group) OR 
                (v.deces IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group)
            ) AS decede,
            -- 4. ASC suivi par AE (en utilisant supervisions) /* 3. Suivi par AE */
            COUNT(DISTINCT r.id) FILTER (WHERE v.suivi_animateur_endogene IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS suivi_animateur_endogene,
            -- 4..1. ASC suivi /* 4. Suivi général */
            COUNT(DISTINCT r.id) FILTER (WHERE (v.suivi IS TRUE OR v.suivi_animateur_endogene IS TRUE) AND v.event_period=p.period AND v.event_age_group=p.age_group) AS suivi_general,
            -- 5. ASC participé à réunion mensuelle /* 5. Réunion mensuelle */
            COUNT(DISTINCT r.id) FILTER (WHERE v.reunion_mensuelle IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS reunion_mensuelle,
            -- 6. ASC produit rapport mensuel /* 6. Rapport mensuel */
            COUNT(DISTINCT r.id) FILTER (WHERE v.rapport_mensuel IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS rapport_mensuel,
            -- 7. ASC raté > 2 mois réunions consécutives et sans rapport /* 7. Raté > 2 mois */
            COUNT(DISTINCT r.id) FILTER (WHERE a.statut_vie <> 'Décédé' AND v.is_active IS TRUE AND v.last_reunion_event_date IS NULL AND AGE(CURRENT_DATE, NULLIF(v.last_rapport_event_date, '')::date) > INTERVAL '2 months' AND v.event_period=p.period AND v.event_age_group=p.age_group) AS missed_2mois,
            -- 9. ASC ayant présenté démission /* 8. Démission */
            COUNT(DISTINCT r.id) FILTER (WHERE v.demission IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS demission,
            -- 10. ASC ayant abandonné /* 9. Abandon */
            COUNT(DISTINCT r.id) FILTER (WHERE v.abandon IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS abandon,
            -- 11. ASC ayant commis faute grave /* 10. Licenciement */
            COUNT(DISTINCT r.id) FILTER (WHERE v.licenciement IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS licenciement,
            -- 11. ASC ayant commis faute grave /* 11. Faute grave */
            COUNT(DISTINCT r.id) FILTER (WHERE v.faute_grave IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS faute_grave,
            -- 12. ASC supervisé /* 12. Supervision */
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervise,
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision_rfs IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervision_rfs,
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision_rm IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervision_rm,
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision_asc_superviseur IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervision_asc_superviseur,
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision_niveau_district IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervision_niveau_district,
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision_niveau_regions IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervision_niveau_regions,
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision_niveau_central IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervision_niveau_central,
            -- 13. ASC supervisé au moins une fois par trimestre /* 13. Supervision au moins 1 fois par trimestre */
            COUNT(DISTINCT r.id) FILTER (WHERE v.supervision IS TRUE AND DATE_TRUNC('quarter', v.last_event_date)=DATE_TRUNC('quarter', CURRENT_DATE) AND v.event_period=p.period AND v.event_age_group=p.age_group) AS supervise_trimestre,
            -- 14. ASC disposant de matériel fonctionnel /* 14. Équipements en bon état */
            COUNT(DISTINCT r.id) FILTER (WHERE v.sac_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS sac_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.velo_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS velo_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.stylos_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS stylos_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.torche_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS torche_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.bottes_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS bottes_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.caisse_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS caisse_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.affiches_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS affiches_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.powerbank_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS powerbank_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.smartphone_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS smartphone_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.thermometre_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS thermometre_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.boites_a_images_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS boites_a_images_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE v.impermeables_raglan_good_state IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS impermeables_raglan_good_state,
            COUNT(DISTINCT r.id) FILTER (WHERE (v.autre_equipement_one_good_state IS TRUE OR v.autre_equipement_two_good_state IS TRUE) AND v.event_period=p.period AND v.event_age_group=p.age_group) AS autres_equipement,
            -- 15. Nombre d’ASC formés
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_pecimne IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_pecimne,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_paludisme IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_paludisme,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_pf_comm IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_pf_communautaire,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_gestion_meg IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_gestion_meg,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_comm_vih IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_comm_vih,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_malnutrition IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_malnutrition,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_pec_pvvih IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_pec_pvvih,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_promotion IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_promotion,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_change_cptm IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_change_cptm,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_assainissement IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_assainissement,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_coinfection_tb IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_coinfection_tb,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_maladi_non_transmissible IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_maladi_non_transmissible,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_Maladi_tropicale IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_Maladi_tropicale,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_suivi_rapportage IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_suivi_rapportage,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_qualite_soins_nc IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_qualite_soins_nc,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_sante_mere IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_sante_mere,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_surveil_epidemiologique IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_surveil_epidemiologique,
            COUNT(DISTINCT r.id) FILTER (WHERE v.formation_others IS TRUE AND v.event_period=p.period AND v.event_age_group=p.age_group) AS formation_others,
            -- 16. Nombre de nouveaux ASC proposé par le niveau opérationnel au cours de l’année
            COUNT(DISTINCT r.id) FILTER (WHERE a.proposition IS TRUE AND r.enrol_period=p.period AND r.enrol_age_group=p.age_group) AS proposition_faite,
            -- 17. Nombre de nouveaux ASC proposé par le niveau opérationnel et validé par le NC
            COUNT(DISTINCT r.id) FILTER (WHERE a.valid_proposition IS TRUE AND r.enrol_period=p.period AND r.enrol_age_group=p.age_group) AS proposition_valide
        -- tu peux continuer ici à ajouter les autres indicateurs de la même manière
        FROM period_base p
            -- Enrollment rows (r) : enrolments_base_mv is assumed to be one row per enrollment
            LEFT JOIN enrollments_base r ON r.orgunit_id=p.orgunit_id AND r.enrol_period=p.period AND r.program=p.program AND r.id IS NOT NULL 
            -- join to latest attributes (one row per enrollment) to obtain sex/status/birth_year etc.
            LEFT JOIN (SELECT DISTINCT ON (enrollment_id) * FROM attributes_latest WHERE sex IS NOT NULL AND status IS NOT NULL ORDER BY enrollment_id DESC) a ON a.enrollment_id = r.id
            -- join events aggregated per enrollment+period
            LEFT JOIN (SELECT DISTINCT ON (enrollment_id, event_period, event_age_group) * FROM events_agg) v ON v.enrollment_id = r.id AND v.event_period = p.period AND v.event_age_group = p.age_group
        WHERE p.program = 'DdjHMnKg3wx' AND p.orgunit_id IS NOT NULL AND p.period IS NOT NULL AND p.age_group IS NOT NULL 
        GROUP BY r.id, r.tei_id, p.orgunit_id, p.period, p.age_group, a.sex, a.status
        ORDER BY r.id, p.orgunit_id, p.period, a.sex;

        CREATE UNIQUE INDEX IF NOT EXISTS indicators_matview_uidx ON indicators_matview (uid);

