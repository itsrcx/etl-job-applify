class ETLPipeline:
    def __init__(self, data_source, transformer, data_store, post_processor, logger):
        self.data_source = data_source
        self.transformer = transformer
        self.data_store = data_store
        self.post_processor = post_processor
        self.logger = logger

    def run(self):
        self.logger.log("Starting ETL Job")
        
        # Step 1: Fetch data
        data = self.data_source.fetch_data()
        self.logger.log("Data fetched")

        # Step 2: Clean and transform data
        transformed_data = self.transformer.clean_data(data)
        self.logger.log("Data cleaned and transformed")

        # Step 3: Upsert data to the target store
        self.data_store.upsert_data(transformed_data)
        self.logger.log("Data upserted to target")

        # Step 4: Post-processing
        self.post_processor.process(transformed_data)
        self.logger.log("Post-processing done")

        # Step 5: Mark job as done
        self.logger.log("ETL Job completed")
