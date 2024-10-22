class PostProcessing:
    def process(self, data):
        raise NotImplementedError()

class AnalyticsPostProcessing(PostProcessing):
    def process(self, data):
        # Perform analytics or ML tasks on the data
        pass
