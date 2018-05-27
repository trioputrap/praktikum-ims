from integration import Integration
import config

tables = (
    {"name": "invoices", "id": "id"},
)

integration = Integration(config.DB_2, config.DB_1, tables)
integration.run()
while 1:
   pass