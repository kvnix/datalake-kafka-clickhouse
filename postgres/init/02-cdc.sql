-- Pré-requisito de CDC: com REPLICA IDENTITY FULL, eventos de UPDATE e DELETE
-- carregam o estado anterior COMPLETO da linha (campo "before" do Debezium).
-- Roda apenas na primeira inicialização do volume (docker-entrypoint-initdb.d).
DO $$
DECLARE
    t record;
BEGIN
    FOR t IN
        SELECT tablename FROM pg_tables WHERE schemaname = 'public'
    LOOP
        EXECUTE format('ALTER TABLE public.%I REPLICA IDENTITY FULL', t.tablename);
        RAISE NOTICE 'REPLICA IDENTITY FULL: public.%', t.tablename;
    END LOOP;
END
$$;
