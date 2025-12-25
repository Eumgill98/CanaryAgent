from typing import Optional, Dict

from langchain_core.prompts import PromptTemplate
from langchain_core.prompt_values import StringPromptValue, PromptValue

class BaseCollector():
    """
    Abstract class for Collector
    """
    def __init__(self):
        self._raw_data: Optional[Dict] = None

    @property
    def get_raw_data(self) -> Optional[Dict]:
        """Return raw data (Dict)"""
        return self._raw_data

    def to_prompt(
        self,
        data: Dict,
        template: Optional[PromptTemplate],
    ) -> PromptValue:
        """
        A method that formats data into prompt values.

        Args:
            data (Dict): Collected data.
            template (Optional[PromptTemplate]): Prompt template to map data. (default=None)

        Returns:
            prompt_value: Completed prompt values.
        """
        if template is None:
            text = "\n".join(f"{k}: {v}" for k, v in data.items())
            return StringPromptValue(text=text)

        return template.invoke(data)