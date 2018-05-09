from integration import Integration
import config

tables = (
    {"name": "invoices", "id": "id"},
)

integration = Integration(config.DB_4, config.DB_5, tables)
integration.run()
while 1:
   pass