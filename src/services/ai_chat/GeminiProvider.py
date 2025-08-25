import time
import google.generativeai as genai

from services.ai_chat.AiProvider import AiProvider
from services.AppData import AppData
from services.logger.Logger import _log


class GeminiProvider(AiProvider):
    """
    Google Gemini AI provider.
    Extends the AiProvider class and implements the ask method to generate content using the Google Gemini API.
    """

    def __init__(self, api_key=None):
        super().__init__()  # Initialize the parent class

        # Set the API key, either from the environment or directly from the parameter
        self.api_key = api_key or AppData().get_api_key("googlegemini")
        if not self.api_key:
            raise ValueError("API key is required for Google Gemini API")

        genai.configure(api_key=self.api_key)
        self.model_name = "gemini-1.5-flash"

    def ask(self, prompt: str) -> dict[str, str]:
        try:
            _log("[Gemini] Generating content...")

            # Count the time taken to generate the content
            start_time = time.time()

            model = genai.GenerativeModel(self.model_name)
            response = model.generate_content(prompt)

            # Calculate the time taken to generate the content
            end_time = time.time()
            time_taken = end_time - start_time

            _log(
                "[Gemini] Content ready! Time taken: {:.2f} seconds".format(time_taken)
            )

            return {"response": response.text, "provider": "Google Gemini"}
        except Exception as e:
            _log(f"{str(e)}", level="ERROR")
            return None
