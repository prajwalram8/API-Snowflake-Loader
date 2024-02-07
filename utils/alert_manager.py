# utils/alert_manager.py

class AlertManager:
    def send_alert(self, message, level='INFO'):
        """
        Send an alert with the given message and level.
        
        :param message: str - The message for the alert.
        :param level: str - The level of the alert (e.g., 'INFO', 'CRITICAL').
        """
