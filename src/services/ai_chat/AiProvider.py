import yaml
import json
import re

from services.AppData import AppData

class AiProvider:
    """
    Abstract class for AI providers.
    Provides methods to interact with an AI provider and generate prompts.
    """

    def __init__(self):
        self.config_file = AppData().get_config("ai_config_file")
        self.reserved_templates = []

        # Initialize empty data
        self.location = None
        self.start_date = None
        self.end_date = None

    def ask(self, prompt: str) -> dict[str, str]:
        """
        Main method to interact with the AI provider

        Args:
            prompt (str): The prompt to send to the AI provider.

        Returns:
            dict[str, str]: The response from the AI provider formatted
            as a dictionary containing a "response" key with the text response.
        """
        raise NotImplementedError("Ask method must be implemented in child class")

    def prompt(self, prompt: str) -> str:
        """
        Prompt the AI provider with a custom prompt, and return the response text.

        Args:
            prompt (str): The custom prompt to send to the AI provider.

        Returns:
            str: The response from the AI provider.
        """
        response = self.ask(prompt=prompt)
        return response.get("response", "")

    def _generate_prompt_from_template(
        self,
        template_key: str = "chat_prompt",
        base_prompt: str = "",
        variables: dict = {}
    ) -> str:
        """
        Generate a prompt from a template, replacing the variables with the data provided on the prepare() method.

        Args:
            template_key (str): The key of the template to use (Retrieved from the config file).
            base_prompt (str): The base prompt to use (If provided, overrides the template_key template).

        Returns:
            str: The final generated prompt.

        """
        # Get the base prompt either from the template or the provided base_prompt
        prompt = (
            base_prompt
            if base_prompt
            else self._load_base_prompt(template_key=template_key)
        )
        
        # Replace variables
        for key, value in variables.items():
            prompt = prompt.replace(f"%%{key}%%", str(value))

        return prompt

    def _to_json(self, response: dict) -> dict:
        """
        Convert the response from the AI provider to a JSON object.

        Args:
            response (dict): The response from the AI provider formatted
            as a dictionary containing a "response" key with the text response.

        Returns:
            dict: The response as a JSON object.
        """
        if not response or "response" not in response:
            return {}

        # Extract the response string
        response_str = response.get("response", "")

        # Use regex to remove the markdown code block formatting
        json_str = re.sub(r"```json\n|\n```", "", response_str).strip()

        # Parse the cleaned JSON string into a Python object
        parsed_json = json.loads(json_str)

        return parsed_json

    def _load_base_prompt(self, template_key: str = "chat_prompt") -> str:
        """
        Load the base prompt from the config file.

        Args:
            template_key (str): The key of the template to load from the config file.

        Returns:
            str: The loaded base prompt.
        """
        if self.get(template_key):
            return self.get(template_key)

        with open(self.config_file, "r", encoding="utf-8") as file:
            data = yaml.safe_load(file)
            return self.set(template_key, data[template_key])

    def _override_base_prompt(
        self, prompt: str = "", template_key: str = "chat_prompt"
    ) -> None:
        """
        Override the base prompt with a custom prompt.

        Args:
            prompt (str): The custom prompt to set
            template_key (str): The key of the template to override
        """
        self.set(template_key, prompt)

    def _strip_reserved_templates(self, text: str) -> str:
        """
        Remove the reserved templates from the text.

        Args:
            text (str): The text to remove the templates from.

        Returns:
            str: The text with the templates removed.
        """
        for template in self.reserved_templates:
            text = text.replace(template, "")
        return text

    def get(self, name):
        """
        Get the value of an attribute by name.

        Args:
            name (str): The name of the attribute to get.

        Returns:
            Any: The value of the attribute.
        """
        return getattr(self, name)

    def set(self, name, value):
        """
        Set the value of an attribute by name.

        Args:
            name (str): The name of the attribute to set.
            value (Any): The value to set.

        Returns:
            Any: The value that was set.
        """
        setattr(self, name, value)
        return value
