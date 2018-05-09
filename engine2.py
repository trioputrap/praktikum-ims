from integration import Integration
import config

tables = (
    {"name": "invoices", "id": "id"},
)

integration = Integration(config.DB_5, config.DB_4, tables)
integration.run()
while 1:
   pass