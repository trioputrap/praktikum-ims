from integration import Integration
import config

integration = Integration(config.DB_2, config.DB_1,["invoice_sync","test"])
integration.run()
while 1:
   pass