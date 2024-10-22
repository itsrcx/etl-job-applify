import boto3
import logging
import time
from datetime import datetime
from botocore.exceptions import ClientError

class Logger:
    def get_logger(self):
        raise NotImplementedError()


class ETLLogger(Logger):
    def __init__(self, log_file='etl_jobs.log'):
        """
        Initialize the ETLLogger.

        Parameters:
            log_file (str): The name of the log file where logs will be saved.
        """
        self.logger = logging.getLogger("ETLLogger")
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler that logs debug and higher level messages
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)

        # Create a console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Create a formatter and set it for both handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        """Return the configured logger instance."""
        return self.logger
    
class CloudWatchLogger(Logger):
    def __init__(self, log_group):
        """Initialize the CloudWatchLogger."""
        self.log_client = boto3.client('logs')
        self.log_group = log_group
        self.sequence_token = None
        
        # Ensure the log group exists
        self._create_log_group()

        # Create the log stream for today's date
        self.log_stream = self._get_daily_log_stream()
        self._create_log_stream()

    def _create_log_group(self):
        """Create the log group if it doesn't exist."""
        try:
            self.log_client.create_log_group(logGroupName=self.log_group)
        except self.log_client.exceptions.ResourceAlreadyExistsException:
            pass  # Log group already exists

    def _get_daily_log_stream(self):
        """Generate a log stream name based on the current date."""
        current_date = datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
        return f"ETLLogStream-{current_date}"

    def _create_log_stream(self):
        """Create the log stream if it doesn't exist."""
        try:
            self.log_client.create_log_stream(logGroupName=self.log_group, logStreamName=self.log_stream)
        except self.log_client.exceptions.ResourceAlreadyExistsException:
            pass

    def log(self, message):
        """Log the message to AWS CloudWatch."""
        timestamp = int(time.time() * 1000)
        log_event = {
            'timestamp': timestamp,
            'message': message
        }

        if self.sequence_token:
            self.log_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                logEvents=[log_event],
                sequenceToken=self.sequence_token
            )
        else:
            response = self.log_client.put_log_events(
                logGroupName=self.log_group,
                logStreamName=self.log_stream,
                logEvents=[log_event]
            )
            self.sequence_token = response['nextSequenceToken']
