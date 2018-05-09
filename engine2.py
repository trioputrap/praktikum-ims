from integration import Integration
import config

tables = (
    {"name": "invoices", "id": "id"},
)

integration = Integration(config.DB_3, config.DB_2, tables)
integration.run()
while 1:
   pass