from dagster import IOManager, io_manager


class NoOpIOManager(IOManager):
    def handle_output(self, context, obj):
        # Do nothing: DB is the system of record
        pass

    def load_input(self, context):
        # Missing upstream → None (OR-bridge relies on this)
        return None


@io_manager
def noop_io_manager():
    return NoOpIOManager()
