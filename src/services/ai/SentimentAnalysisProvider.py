from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
import torch
import time

from services.ai.AiProvider import AiProvider
from services.AppData import AppData
from services.log.Logger import _log


class SentimentAnalyzer(AiProvider):
    """
    Sentiment analysis provider using the Hugging Face pipeline.
    Extends the AiProvider class and implements the ask method to perform sentiment analysis
    using the "pysentimiento/bertweet-pt-sentiment" model.
    """

    def __init__(self):
        super().__init__()  # Initialize the parent class

        # Device configuration: use GPU if available, otherwise fallback to CPU
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Define model details and load the sentiment analysis pipeline
        self.model_name = "pysentimiento/bertweet-pt-sentiment"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)

        # Define the pipeline for sentiment analysis
        self.pipe = pipeline(
            task="sentiment-analysis",
            model=self.model,
            tokenizer=self.tokenizer,
            device=(
                0 if torch.cuda.is_available() else -1
            ),  # Set device for the pipeline
        )

    def ask(self, prompt: str) -> dict[str, str]:
        try:
            _log("[HuggingFace] Performing sentiment analysis...")

            # Count the time taken to generate the content
            start_time = time.time()

            # Perform sentiment analysis
            response = self.pipe(prompt)

            # Calculate the time taken to generate the content
            end_time = time.time()
            time_taken = end_time - start_time

            _log(
                "[HuggingFace] Analysis is ready! Time taken: {:.2f} seconds".format(
                    time_taken
                )
            )

            # Return the response
            return {
                "response": self._format_response(prompt, response),
                "provider": "SentimentAnalyzer",
            }
        except Exception as e:
            _log(f"Error during sentiment analysis: {str(e)}", level="ERROR")
            return None

    def _format_response(self, prompt: str, response: list[dict]) -> str:
        """
        Format the sentiment analysis result into the desired format.

        Args:
            prompt (str): The input text for sentiment analysis
            response (list[dict]): The sentiment analysis response

        Returns:
            str: The formatted sentiment analysis result
        """
        # Format the sentiment analysis result into a readable string
        sentiment = response[0]["label"]
        confidence = response[0]["score"]

        # Map the sentiment label to ensure it matches POSITIVE, NEGATIVE, or NEUTRAL
        sentiment_mapping = {
            "POS": "POSITIVE",
            "NEG": "NEGATIVE",
            "NEU": "NEUTRAL",
        }
        sentiment = sentiment_mapping.get(sentiment.upper(), "UNKNOWN")

        return f"Text: '{prompt}'\nSentiment: {sentiment}\nConfidence: {confidence:.2f}"

    def analyze_sentiment(self, text: str) -> str:
        """
        Analyze the sentiment of the given text.

        Args:
            text (str): The text to analyze

        Returns:
            str: The sentiment of the text (POSITIVE, NEGATIVE, NEUTRAL)
        """
        response = self.ask(prompt=text)
        if not response:
            return None
        response = response["response"]
        response = response.split("Sentiment:")[1].strip()
        return response.split("\n")[0].strip().upper()

    def _clean_response(self, response: str) -> str:
        return response.strip()
