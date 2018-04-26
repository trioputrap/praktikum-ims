from integration import Integration
import config

integration = Integration(config.DB_1, config.DB_2,["invoice_sync"])
integration.run()
while 1:
   pass