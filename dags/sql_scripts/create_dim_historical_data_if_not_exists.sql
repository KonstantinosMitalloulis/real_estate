DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'real_estate' AND table_name = 'dim_historical_data') THEN
        CREATE TABLE real_estate.dim_historical_data (
            webpage varchar(140) PRIMARY KEY
        );
    END IF;
END $$;