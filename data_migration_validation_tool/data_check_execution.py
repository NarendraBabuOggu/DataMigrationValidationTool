from typing import Dict, List, Tuple, Callable, Union


class DataCheckExecution:
    def __init__(self, config: Dict[str, Dict[str, Union[str, Dict]]]):
        self.config = config
