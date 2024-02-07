# state_manager/state_manager.py
import json
import os

class StateManager():
    def __init__(self, name,json_path='.', json_file='state.json',default_start='2024-01-01') -> None:
        self.default_start = default_start
        self.json_file = os.path.join(json_path,json_file)
        self.name = name

    def get_last_state(self):
        """
        Retrieve the last state of the data load, which could be a timestamp or an ID.
        
        :return: str or int - The last state.
        """
        try:
            with open(self.json_file, 'r') as file:
                state = json.load(file)
                return state.get(self.name).get('last_run', self.default_start)
        except FileNotFoundError:
            return self.default_start
    
    def update_state(self, last_run_date):
        """
        Update the state with the given new state.
        
        :param last_run_date: str - The new last run date to save.
        """
        try:
            # First, open the file in 'r' mode to read the current state.
            with open(self.json_file, 'r') as file:
                state = json.load(file)
        except FileNotFoundError:
            # If the file does not exist, initialize the state.
            state = {}
        except json.JSONDecodeError:
            # If the file is empty or contains invalid JSON, reset the state.
            state = {}

        # Update the state in memory.
        if self.name not in state:
            state[self.name] = {}
        state[self.name]['last_run'] = last_run_date

        # Then, open the file in 'w' mode to write the updated state back.
        with open(self.json_file, 'w') as file:
            json.dump(state, file, indent=4)
