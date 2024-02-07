# scheduler/scheduler.py

class Scheduler:
    def schedule_task(self, interval, function, *args):
        """
        Schedule a task to run at a fixed interval.
        
        :param interval: int - The interval in seconds between task executions.
        :param function: callable - The function to execute.
        :param args: list - The arguments to pass to the function.
        """
