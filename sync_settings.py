# -*- coding: utf-8 -*-
from odoo import api, fields, models
from odoo.exceptions import UserError
import psycopg2
import psycopg2.extras
import logging
_logger = logging.getLogger(__name__)


class SyncSettings(models.Model):
    _name = 'sync.settings'
    _description = 'Sync Settings'
    _order = 'is_default, name ASC'

    name = fields.Char('Name', required=True, help='Connection Name')
    host = fields.Char('Host', required=True, help='IP address to connect')
    db_name = fields.Char('Database Name', required=True, help='Database Name')
    username = fields.Char('Username', required=True, help='Username')
    password = fields.Char('Password', help='Password')
    port = fields.Integer('Port', required=True, default=22)
    is_default = fields.Boolean('Is Default?', default=False, help='Is default connection')

    @api.multi
    def button_set_default(self):
        other_ids = self.search([('id', 'not in', self.ids)])
        other_ids.write({'is_default': False})
        self.write({'is_default': True})

    @api.multi
    def button_test(self):
        cr = False
        conn = False
        try:
            conn = psycopg2.connect(dbname=self.db_name, user=self.username, password=self.password, host=self.host, port=self.port)
            cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';"  # Just select all table names
            cr.execute(query)
            cr.fetchall()
        except Exception, e:
            raise UserError('Connection Failed!\n' + str(e))
        finally:
            try:
                cr.close()
                conn.close()
            except Exception:
                pass
        raise UserError('Connection Test Succeeded! Everything seems properly set up!')

    # Importing Customer/ Supplier data
    @api.multi
    def cron_process_import_res_partner(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # res_partner import
                self.import_res_partner(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error: Customer/ Vendor %s\n' % str(e))  #
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # imporing Product Category
    @api.multi
    def cron_process_import_product_category(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # product_category import
                self.import_product_category(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Product Category: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # import Product UOM
    @api.multi
    def cron_process_import_product_uom(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # product_uom import
                self.import_product_uom(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Product UOM: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # import Product Template
    @api.multi
    def cron_process_import_product_template(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # product_template import
                self.import_product_template(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Product Template: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # import Stock Warehouse
    @api.multi
    def cron_process_import_stock_warehouse(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # stock_warehouse import
                self.import_stock_warehouse(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Warehouse: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # import sale Orders
    @api.multi
    def cron_process_import_sale_order(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # sale_order import
                self.import_sale_order(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Sale Order: %s\n' % str(e))  #
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # import Purchase Orders
    @api.multi
    def cron_process_import_purchase_order(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # purchase_order import
                self.import_purchase_order(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Purchase order: %s\n' % str(e))  #
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Import Stock_inventory
    @api.multi
    def cron_process_import_stock_inventory(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # Stock Adjustment import
                self.import_stock_inventory(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Inventory Adjustment: %s\n' % str(e))  #
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Import Internal Transfer
    @api.multi
    def cron_process_import_internal_transfer(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # internal transfer import
                self.import_internal_transfer(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Intenral Transfer: %s\n' % str(e))  #
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Import Hr Expense
    @api.multi
    def cron_process_import_hr_expense(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # hr_expense import
                self.import_hr_expense(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Hr Expense: %s\n' % str(e))  #
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Import Customer Status from ODOO to Postgres
    @api.multi
    def cron_process_import_customer_status(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # hr_expense import
                self.import_customer_status(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Customer Status: %s\n' % str(e))  #
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    @api.multi
    def import_res_partner(self, cr, conn):
        # Process columns
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='res_partner';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM res_partner WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['company_type'] = 'company'
                vals['name'] = line[columns.index('name')]
                vals['active'] = line[columns.index('active')]
                vals['street'] = line[columns.index('street')]
                vals['street2'] = line[columns.index('street2')]
                vals['city'] = line[columns.index('city')]
                region_id = False
                if line[columns.index('tin')]:
                    vals['npwp'] = line[columns.index('tin')]
                if line[columns.index('company')]:
                    company_id = self.env['res.partner.company.type'].search([('name', '=', line[columns.index('company')])], limit=1)
                    if not company_id:
                        company_id = self.env['res.partner.company.type'].create({'name': line[columns.index('company')]})
                    vals['partner_company_type_id'] = company_id.id
                if line[columns.index('country_id')]:
                    country_id = self.env['res.country'].search([('name', 'ilike', line[columns.index('country_id')])], limit=1)
                    if not country_id:
                        country_id = self.env['res.country'].create({'name': line[columns.index('country_id')]})
                    vals['country_id'] = country_id.id
                if line[columns.index('state_id')]:
                    state_id = self.env['res.country.state'].search([('name', 'ilike', line[columns.index('state_id')])], limit=1)
                    if not state_id:
                        state_id = self.env['res.country.state'].create({
                            'name': line[columns.index('state_id')],
                            'country_id': country_id.id,
                            'code': line[columns.index('state_id')]
                        })
                    vals['state_id'] = state_id.id
                if line[columns.index('region')]:
                    region_id = self.env['res.partner.region'].search([('name', 'ilike', line[columns.index('region')])], limit=1)
                    if not region_id:
                        region_id = self.env['res.partner.region'].create({'name': line[columns.index('region')]})
                    vals['region_id'] = region_id.id
                if line[columns.index('area')]:
                    area_id = self.env['res.partner.area'].search([('name', 'ilike', line[columns.index('area')])], limit=1)
                    if not region_id:
                        region_id = self.env['res.partner.region'].search([], limit=1)
                    if not area_id:
                        area_id = self.env['res.partner.area'].create({'name': line[columns.index('area')], 'region_id': region_id.id})
                    vals['area_id'] = area_id.id
                if line[columns.index('partner_analytic_acc')]:
                    analytic_account_id = self.env['account.analytic.account'].search([('name', '=', line[columns.index('partner_analytic_acc')])], limit=1)
                    if not analytic_account_id:
                        analytic_account_id = self.env['account.analytic.account'].create({'name': line[columns.index('partner_analytic_acc')]})
                    vals['analytic_name'] = analytic_account_id.id
                vals['zip'] = line[columns.index('zip')]
                vals['email'] = line[columns.index('email')]
                vals['phone'] = line[columns.index('phone')]
                vals['mobile'] = line[columns.index('mobile')]
                vals['fax'] = line[columns.index('fax')]
                vals['supplier'] = line[columns.index('supplier')]
                vals['customer'] = line[columns.index('customer')]
                vals['type'] = line[columns.index('type')]
                vals['credit_limit'] = line[columns.index('credit_limit')]
                vals['debit_limit'] = line[columns.index('debit_limit')]
                user_id = self.env['res.users'].search([('name', 'ilike', line[columns.index('user_id')])], limit=1)
                vals['user_id'] = user_id.id if user_id else False
                partner = self.env['res.partner'].create(vals)
                # Update powerone table
                query = 'UPDATE res_partner SET odoo_id=%s WHERE id=%s;' % (partner.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE res_partner SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import res.partner error : %s' % str(e))
        return True

    # Importing updated Customer master data from Postgres to ODOO
    def cron_process_update_customer_master_data(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # res_partner import
                self.import_updated_customer_master_data(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Updated Customer/ Supplier: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Importing updated Customer master data from Postgres to ODOO
    def import_updated_customer_master_data(self, cr, conn):
        # Process columns
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='res_partner';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM res_partner WHERE odoo_id is NOT NULL and is_update_data = True LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['company_type'] = 'company'
                vals['name'] = line[columns.index('name')]
                vals['active'] = line[columns.index('active')]
                vals['street'] = line[columns.index('street')]
                vals['street2'] = line[columns.index('street2')]
                vals['city'] = line[columns.index('city')]
                if line[columns.index('tin')]:
                    vals['npwp'] = line[columns.index('tin')]
                if line[columns.index('company')]:
                    company_id = self.env['res.partner.company.type'].search([('name', '=', line[columns.index('company')])], limit=1)
                    if not company_id:
                        company_id = self.env['res.partner.company.type'].create({'name': line[columns.index('company')]})
                    vals['partner_company_type_id'] = company_id.id
                if line[columns.index('country_id')]:
                    country_id = self.env['res.country'].search([('name', 'ilike', line[columns.index('country_id')])], limit=1)
                    if not country_id:
                        country_id = self.env['res.country'].create({'name': line[columns.index('country_id')]})
                    vals['country_id'] = country_id.id
                if line[columns.index('state_id')]:
                    state_id = self.env['res.country.state'].search([('name', 'ilike', line[columns.index('state_id')])], limit=1)
                    if not state_id:
                        state_id = self.env['res.country.state'].create({
                            'name': line[columns.index('state_id')],
                            'country_id': country_id.id,
                            'code': line[columns.index('state_id')]
                        })
                    vals['state_id'] = state_id.id
                if line[columns.index('region')]:
                    region_id = self.env['res.partner.region'].search([('name', 'ilike', line[columns.index('region')])], limit=1)
                    if not region_id:
                        region_id = self.env['res.partner.region'].create({'name': line[columns.index('region')]})
                    vals['region_id'] = region_id.id
                if line[columns.index('area')]:
                    area_id = self.env['res.partner.area'].search([('name', 'ilike', line[columns.index('area')])], limit=1)
                    if not region_id:
                        region_id = self.env['res.partner.region'].search([], limit=1)
                    if not area_id:
                        area_id = self.env['res.partner.area'].create({'name': line[columns.index('area')], 'region_id': region_id.id})
                    vals['area_id'] = area_id.id
                if line[columns.index('partner_analytic_acc')]:
                    analytic_account_id = self.env['account.analytic.account'].search([('name', '=', line[columns.index('partner_analytic_acc')])], limit=1)
                    if not analytic_account_id:
                        analytic_account_id = self.env['account.analytic.account'].create({'name': line[columns.index('partner_analytic_acc')]})
                    vals['analytic_name'] = analytic_account_id.id
                vals['zip'] = line[columns.index('zip')]
                vals['email'] = line[columns.index('email')]
                vals['phone'] = line[columns.index('phone')]
                vals['mobile'] = line[columns.index('mobile')]
                vals['fax'] = line[columns.index('fax')]
                vals['supplier'] = line[columns.index('supplier')]
                vals['customer'] = line[columns.index('customer')]
                vals['type'] = line[columns.index('type')]
                vals['credit_limit'] = line[columns.index('credit_limit')]
                vals['debit_limit'] = line[columns.index('debit_limit')]
                user_id = self.env['res.users'].search([('name', 'ilike', line[columns.index('user_id')])], limit=1)
                vals['user_id'] = user_id.id if user_id else False
                partner = self.env['res.partner'].search([('id', '=', line[columns.index('odoo_id')])], limit=1)
                partner.write(vals)
                # Update powerone table
                query = 'UPDATE res_partner SET is_update_data = False WHERE odoo_id=%s;' % (partner.id)
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting is_update_data as False to skip it permanently
                query = 'UPDATE res_partner SET is_update_data = False WHERE odoo_id=%s;' % (partner.id)
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import res.partner(update) error : %s' % str(e))
        return True

    # Importing updated Product UOM data from Postgres to ODOO
    def cron_process_update_product_uom(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # updated product UOM import
                self.import_updated_product_uom(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Updated Product UOM: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Importing Updated product UOM data from Postgress to ODOO
    def import_updated_product_uom(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='product_uom';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM product_uom WHERE odoo_id is NOT NULL and is_update_data = True LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['factor'] = 100.000
                vals['uom_type'] = 'reference'
                vals['rounding'] = line[columns.index('rounding')]
                if line[columns.index('category_id')]:
                    category_id = self.env['product.uom.categ'].search([('name', '=', line[columns.index('category_id')])], limit=1)
                    if not category_id:
                        category_id = self.env['product.uom.categ'].create({'name': line[columns.index('category_id')]})
                    vals['category_id'] = category_id.id
                # Create product_uom in odoo
                product_uom = self.env['product.uom'].search([('id', '=', line[columns.index('odoo_id')])], limit=1)
                product_uom.write(vals)
                # Update powerone table
                query = 'UPDATE product_uom SET is_update_data = False WHERE odoo_id=%s;' % (product_uom.id)
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting is_update_data as False to skip it permanently
                query = 'UPDATE product_uom SET is_update_data = False WHERE odoo_id=%s;' % (product_uom.id)
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import product.uom(update) error : %s' % str(e))
        return True

    # Importing updated Product Category data from Postgres to ODOO
    def cron_process_update_product_category(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # updated product category import
                self.import_updated_product_category(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Updated Product Category: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Importing updated Inventory Adjustment data from ODOO to Postgress
    def cron_process_update_inventory_adjustment(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # updated inventory Adjustment import
                self.import_inventory_adjustment(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Insert Inventory Adjustments: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Importing Delivery Orders data from Postgres to ODOO
    def cron_process_import_delivery_orders(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                #  Delivery Orders import
                self.import_delivery_order(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Delivery Orders: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Importing Updated product Category data from Postgress to ODOO
    def import_updated_product_category(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='product_category';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM product_category WHERE odoo_id is NOT NULL and is_update_data = True LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                if line[columns.index('parent_id')]:
                    parent_id = self.env['product.category'].search([('name', 'ilike', line[columns.index('parent_id')])], limit=1)
                    if not parent_id:
                        parent_id = self.env['product.category'].create({'name': line[columns.index('parent_id')]})
                    vals['parent_id'] = parent_id.id
                # Create product_category in odoo
                product_category = self.env['product.category'].search([('id', 'ilike', line[columns.index('odoo_id')])], limit=1)
                product_category.write(vals)
                # Update powerone table
                query = 'UPDATE product_category SET is_update_data = False WHERE odoo_id=%s;' % (product_category.id)
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting is_update_data as False to skip it permanently
                query = 'UPDATE product_category SET is_update_data = False WHERE odoo_id=%s;' % (product_category.id)
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import product.category(update) error : %s' % str(e))
        return True

    # Importing updated Product Template data from Postgres to ODOO
    def cron_process_update_product_template(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # res_partner import
                self.import_updated_product_template(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Updated Product Template: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Importing Updated product Template data from Postgress to ODOO
    def import_updated_product_template(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='product_template';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM product_template WHERE odoo_id is NOT NULL and is_update_data = True LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['type'] = 'product'
                if line[columns.index('categ_id')]:
                    categ_id = self.env['product.category'].search([('name', 'ilike', line[columns.index('categ_id')])], limit=1)
                    if not categ_id:
                        categ_id = self.env['product.category'].create({'name': line[columns.index('categ_id')]})
                    vals['categ_id'] = categ_id.id
                vals['default_code'] = line[columns.index('default_code')]
                vals['list_price'] = line[columns.index('list_price')]
                vals['standard_price'] = line[columns.index('standard_price')]
                if line[columns.index('uom_id')]:
                    uom_id = self.env['product.uom'].search([('name', 'ilike', line[columns.index('uom_id')])], limit=1)
                    if not uom_id:
                        uom_id = self.env['product.uom'].create({'name': line[columns.index('uom_id')]})
                    vals['uom_id'] = uom_id.id
                # Create product_template in odoo
                product_template = self.env['product.template'].search([('id', '=', line[columns.index('odoo_id')])], limit=1)
                product_template.write(vals)
                # Update powerone table
                query = 'UPDATE product_template SET is_update_data = False WHERE odoo_id=%s;' % (product_template.id)
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting is_update_data as False to skip it permanently
                query = 'UPDATE product_template SET is_update_data = False WHERE odoo_id=%s;' % (product_template.id)
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import product.template(update) error : %s' % str(e))
        return True

    # Importing updated Stock Warehouse data from Postgres to ODOO
    def cron_process_update_stock_warehouse(self):
        config = self.search([('is_default', '=', True)])
        if config:
            cr = False
            conn = False
            try:
                conn = psycopg2.connect(dbname=config.db_name, user=config.username, password=config.password, host=config.host, port=config.port)
                cr = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                # res_partner import
                self.import_updated_stock_warehouse(cr, conn)
            except Exception, e:
                _logger.error('\nPowerone sync Error Updated Stock Warehouse: %s\n' % str(e))
            finally:
                try:
                    cr.close()
                    conn.close()
                except Exception:
                    pass
        return True

    # Importing Updated Stock Warehouse data from Postgress to ODOO
    def import_updated_stock_warehouse(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='stock_warehouse';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM stock_warehouse WHERE odoo_id is NOT NULL and is_update_data = True LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['code'] = line[columns.index('code')]
                # Create warehouse in odoo
                warehouse = self.env['stock.warehouse'].search([('id', '=', line[columns.index('odoo_id')])], limit=1)
                warehouse.write(vals)
                # Update powerone table
                query = 'UPDATE stock_warehouse SET is_update_data = False WHERE odoo_id=%s;' % (warehouse.id)
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting is_update_data as False to skip it permanently
                query = 'UPDATE stock_warehouse SET is_update_data = False WHERE odoo_id=%s;' % (warehouse.id)
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import stock.warehouse(update) error : %s' % str(e))
        return True

    # Importing Customer status from odoo to postgres
    @api.multi
    def import_customer_status(self, cr, conn):
        # Process columns
        partner_ids = self.env['res.partner'].search([('transaction_status', '!=', False)])
        customer_status = {'normal': 0, 'over_limit': 1, 'over_due': 2, 'over_limit_over_due': 3}
        for partner_id in partner_ids:
            try:
                query = 'UPDATE res_partner SET customer_status=%s WHERE odoo_id=%s;' % (customer_status.get(partner_id.transaction_status, 0), partner_id.id)
                cr.execute(query)
                conn.commit()
            except Exception, e:
                _logger.error('\nPowerone sync import customer.status(update) error : %s' % str(e))
        return True

    @api.multi
    def import_product_category(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='product_category';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM product_category WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                if line[columns.index('parent_id')]:
                    parent_id = self.env['product.category'].search([('name', 'ilike', line[columns.index('parent_id')])], limit=1)
                    if not parent_id:
                        parent_id = self.env['product.category'].create({'name': line[columns.index('parent_id')]})
                    vals['parent_id'] = parent_id.id
                # Create product_category in odoo
                product_category = self.env['product.category'].create(vals)
                # Update powerone table
                query = 'UPDATE product_category SET odoo_id=%s WHERE id=%s;' % (product_category.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE product_category SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import product.category error : %s' % str(e))
        return True

    @api.multi
    def import_product_uom(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='product_uom';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM product_uom WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['factor'] = 100.000
                vals['uom_type'] = 'reference'
                vals['rounding'] = line[columns.index('rounding')]
                if line[columns.index('category_id')]:
                    category_id = self.env['product.uom.categ'].search([('name', '=', line[columns.index('category_id')])], limit=1)
                    if not category_id:
                        category_id = self.env['product.uom.categ'].create({'name': line[columns.index('category_id')]})
                    vals['category_id'] = category_id.id
                # Create product_uom in odoo
                product_uom = self.env['product.uom'].create(vals)
                # Update powerone table
                query = 'UPDATE product_uom SET odoo_id=%s WHERE id=%s;' % (product_uom.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE product_uom SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import product.uom error : %s' % str(e))
        return True

    @api.multi
    def import_product_template(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='product_template';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM product_template WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['type'] = 'product'
                if line[columns.index('categ_id')]:
                    categ_id = self.env['product.category'].search([('name', 'ilike', line[columns.index('categ_id')])], limit=1)
                    if not categ_id:
                        categ_id = self.env['product.category'].create({'name': line[columns.index('categ_id')]})
                    vals['categ_id'] = categ_id.id
                vals['default_code'] = line[columns.index('default_code')]
                vals['list_price'] = line[columns.index('list_price')]
                vals['standard_price'] = line[columns.index('standard_price')]
                if line[columns.index('uom_id')]:
                    uom_id = self.env['product.uom'].search([('name', 'ilike', line[columns.index('uom_id')])], limit=1)
                    if not uom_id:
                        uom_id = self.env['product.uom'].create({'name': line[columns.index('uom_id')]})
                    vals['uom_id'] = uom_id.id
                # Create product_template in odoo
                product_template = self.env['product.template'].create(vals)
                # Update powerone table
                query = 'UPDATE product_template SET odoo_id=%s WHERE id=%s;' % (product_template.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE product_template SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import product.template error : %s' % str(e))
        return True

    @api.multi
    def import_stock_warehouse(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='stock_warehouse';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM stock_warehouse WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                warehouse_id = self.env['stock.warehouse'].search([('code', '=', line[columns.index('code')])])
                if not warehouse_id:
                    vals = {}
                    vals['name'] = line[columns.index('name')]
                    vals['code'] = line[columns.index('code')]
                    vals['active'] = True
                    # Create warehouse in odoo
                    warehouse = self.env['stock.warehouse'].create(vals)
                    # Update powerone table
                    query = 'UPDATE stock_warehouse SET odoo_id=%s WHERE id=%s;' % (warehouse.id, line[columns.index('id')])
                    cr.execute(query)
                    conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE stock_warehouse SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import stock.warehouse error : %s' % str(e))
        return True

    @api.multi
    def import_sale_order(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='sale_order';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM sale_order WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['client_order_ref'] = line[columns.index('order_ref')]
                vals['state'] = 'draft'
                vals['date_order'] = line[columns.index('date_order')]
                if line[columns.index('partner_id')]:
                    partner_id = self.env['res.partner'].search([('name', '=', line[columns.index('partner_id')])], limit=1)
                    if not partner_id:
                        partner_id = self.env['res.partner'].create({'name': line[columns.index('partner_id')]})
                    vals['partner_id'] = partner_id.id
                    vals['project_id'] = partner_id.analytic_name.id
                if line[columns.index('pricelist_id')]:
                    pricelist_id = self.env['product.pricelist'].search([('name', '=', line[columns.index('pricelist_id')])], limit=1)
                    if not pricelist_id:
                        pricelist_id = self.env['product.pricelist'].create({'name': line[columns.index('pricelist_id')]})
                    vals['pricelist_id'] = pricelist_id.id
                if line[columns.index('payment_term_id')]:
                    payment_term_id = self.env['account.payment.term'].search([('name', '=', line[columns.index('payment_term_id')])], limit=1)
                    if not payment_term_id:
                        payment_term_id = self.env['account.payment.term'].create({'name': line[columns.index('payment_term_id')]})
                    vals['payment_term_id'] = payment_term_id.id
                if line[columns.index('team_id')]:
                    team_id = self.env['crm.team'].search([('name', '=', line[columns.index('team_id')])], limit=1)
                    if not team_id:
                        team_id = self.env['crm.team'].create({'name': line[columns.index('team_id')]})
                    vals['team_id'] = team_id.id
                if line[columns.index('user_id')]:
                    user_id = self.env['res.users'].search([('name', '=', line[columns.index('user_id')])], limit=1)
                    if not user_id:
                        user_id = self.env['res.users'].create({'name': line[columns.index('user_id')], 'login': line[columns.index('user_id')]})
                    vals['user_id'] = user_id.id
                # Create sale_order in odoo
                sale_order = self.env['sale.order'].create(vals)

                order_line_vals = {}
                categ_id = self.env['product.category'].search([('name', 'ilike', 'All')], limit=1)
                if line[columns.index('product_id')]:
                    product_id = self.env['product.product'].search([('name', 'ilike', line[columns.index('product_id')])], limit=1)
                    if not product_id:
                        product_id = self.env['product.product'].create({'name': line[columns.index('product_id')], 'categ_id': categ_id.id})
                    order_line_vals['product_id'] = product_id.id
                    order_line_vals['name'] = product_id.name
                order_line_vals['product_uom_qty'] = line[columns.index('product_uom_qty')]
                order_line_vals['price_unit'] = line[columns.index('price_unit')]
                order_line_vals['discount'] = line[columns.index('discount')]
                order_line_vals['order_id'] = sale_order.id
                if line[columns.index('product_uom')]:
                    categ_id = self.env['product.category'].search([('name', 'ilike', 'All')], limit=1)
                    product_uom = self.env['product.uom'].search([('name', '=', line[columns.index('product_uom')])], limit=1)
                    if not product_uom:
                        product_uom = self.env['product.uom'].create(
                            {'name': line[columns.index('product_uom')], 'category_id': categ_id.id})
                    order_line_vals['product_uom'] = product_uom.id

                # Create sale_order_line in odoo
                self.env['sale.order.line'].create(order_line_vals)
                # Update powerone table
                query = 'UPDATE sale_order SET odoo_id=%s WHERE id=%s;' % (sale_order.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                sale_order.action_confirm()
                for picking_obj in sale_order.picking_ids:
                    wiz = self.env['stock.immediate.transfer'].create({'pick_id': picking_obj.id})
                    wiz.process()
                sale_order.action_invoice_create(final=True)
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE sale_order SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import sale.order : %s' % str(e))
        return True

    @api.multi
    def import_purchase_order(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='purchase_order';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM purchase_order WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                if line[columns.index('name')]:
                    vals['name'] = line[columns.index('name')]
                else:
                    vals['name'] = ' '
                vals['state'] = 'draft'
                vals['date_order'] = line[columns.index('date_order')]
                vals['date_planned'] = line[columns.index('date_planned')]
                vals['notes'] = line[columns.index('notes')]
                if line[columns.index('partner_id')]:
                    partner_id = self.env['res.partner'].search([('name', '=', line[columns.index('partner_id')])], limit=1)
                    if not partner_id:
                        partner_id = self.env['res.partner'].create({'name': line[columns.index('partner_id')]})
                    vals['partner_id'] = partner_id.id
                # Create purchase_order in odoo
                purchase_order = self.env['purchase.order'].create(vals)

                order_line_vals = {}
                if line[columns.index('product_id')]:
                    product_id = self.env['product.product'].search([('name', 'ilike', line[columns.index('product_id')])], limit=1)
                    categ_id = self.env['product.category'].search([('name', 'ilike', 'All')], limit=1)
                    if not product_id:
                        product_id = self.env['product.product'].create({'name': line[columns.index('product_id')], 'categ_id': categ_id.id})
                    order_line_vals['product_id'] = product_id.id
                    order_line_vals['product_uom'] = product_id.uom_id.id
                    if line[columns.index('order_line_description')]:
                        order_line_vals['name'] = line[columns.index('order_line_description')]
                    else:
                        order_line_vals['name'] = product_id.name
                order_line_vals['product_qty'] = line[columns.index('product_qty')]
                order_line_vals['price_unit'] = line[columns.index('price_unit')]
                order_line_vals['date_planned'] = line[columns.index('date_planned')]
                order_line_vals['order_id'] = purchase_order.id

                # Create purchase_order_line in odoo
                self.env['purchase.order.line'].create(order_line_vals)
                purchase_order.button_confirm()
                for picking_obj in purchase_order.picking_ids:
                    wiz = self.env['stock.immediate.transfer'].create({'pick_id': picking_obj.id})
                    wiz.process()
                # Update powerone table
                query = 'UPDATE purchase_order SET odoo_id=%s WHERE id=%s;' % (purchase_order.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE purchase_order SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import purchase.order : %s' % str(e))
        return True

    @api.multi
    def import_stock_inventory(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='stock_inventory';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM stock_inventory WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                if line[columns.index('location_id')]:
                    location_id = self.env['stock.location'].search([('name', 'ilike', line[columns.index('location_id')])], limit=1)
                    if not location_id:
                        location_id = self.env['stock.location'].create({'name': line[columns.index('location_id')], 'usage': 'internal'})
                    vals['location_id'] = location_id.id
                vals['date'] = line[columns.index('inv_date')]
                vals['filter'] = line[columns.index('filter')] or 'none'
                vals['state'] = line[columns.index('state')]
                # Create stock_inventory in odoo
                stock_inventory = self.env['stock.inventory'].create(vals)
                order_line_vals = {}
                if line[columns.index('product_id')]:
                    product_id = self.env['product.product'].search([('name', 'ilike', line[columns.index('product_id')])], limit=1)
                    if not product_id:
                        categ_id = self.env['product.category'].search([('name', 'ilike', 'All')], limit=1)
                        product_id = self.env['product.product'].create({'name': line[columns.index('product_id')], 'categ_id': categ_id.id})
                    order_line_vals['product_id'] = product_id.id
                    order_line_vals['product_uom_id'] = product_id.uom_id.id
                order_line_vals['inventory_id'] = stock_inventory.id
                location_id = self.env['stock.location'].search([('name', '=', 'Stock')], limit=1)
                if location_id:
                    order_line_vals['location_id'] = location_id.id
                # Create stock_inventory_line in odoo
                self.env['stock.inventory.line'].create(order_line_vals)
                # Update powerone table
                query = 'UPDATE stock_inventory SET odoo_id=%s WHERE id=%s;' % (stock_inventory.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE stock_inventory SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import stock.inventory : %s' % str(e))
        return True

    @api.multi
    def import_internal_transfer(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='internal_transfer';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM internal_transfer WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['schedule_date'] = fields.Datetime.now()
                if line[columns.index('source_loc_id')]:
                    source_loc_id = self.env['stock.location'].search([('name', 'ilike', line[columns.index('source_loc_id')])], limit=1)
                    if not source_loc_id:
                        source_loc_id = self.env['stock.location'].create({'name': line[columns.index('source_loc_id')], 'usage': 'internal'})
                    vals['source_loc_id'] = source_loc_id.id
                if line[columns.index('dest_loc_id')]:
                    dest_loc_id = self.env['stock.location'].search([('name', 'ilike', line[columns.index('dest_loc_id')])], limit=1)
                    if not dest_loc_id:
                        dest_loc_id = self.env['stock.location'].create({'name': line[columns.index('dest_loc_id')], 'usage': 'internal'})
                    vals['dest_loc_id'] = dest_loc_id.id

                if line[columns.index('picking_type_outgoing_id')]:
                    picking_type_outgoing_id = self.env['stock.picking.type'].search([('name', '=', line[columns.index('picking_type_outgoing_id')])], limit=1)
                    if not picking_type_outgoing_id:
                        picking_type_outgoing_id = self.env['stock.picking.type'].create({'name': line[columns.index('picking_type_outgoing_id')]})
                    vals['picking_type_outgoing_id'] = picking_type_outgoing_id.id
                if line[columns.index('picking_type_incoming_id')]:
                    picking_type_incoming_id = self.env['stock.picking.type'].search([('name', '=', line[columns.index('picking_type_incoming_id')])], limit=1)
                    if not picking_type_incoming_id:
                        picking_type_outgoing_id = self.env['stock.picking.type'].create({'name': line[columns.index('picking_type_incoming_id')]})
                    vals['picking_type_incoming_id'] = picking_type_incoming_id.id
                vals['state'] = 'draft'
                # Create internal_transfer in odoo
                internal_transfer = self.env['internal.transfer'].create(vals)

                order_line_vals = {}
                if line[columns.index('product_id')]:
                    product_id = self.env['product.product'].search([('name', 'ilike', line[columns.index('product_id')])], limit=1)
                    if not product_id:
                        categ_id = self.env['product.category'].search([('name', 'ilike', 'All')], limit=1)
                        product_id = self.env['product.product'].create({'name': line[columns.index('product_id')], 'categ_id': categ_id.id})
                    order_line_vals['product_id'] = product_id.id
                    order_line_vals['uom_id'] = product_id.uom_id.id
                order_line_vals['product_uom_qty'] = line[columns.index('product_uom_qty')]
                order_line_vals['transfer_id'] = internal_transfer.id
                # Create stock_inventory_line in odoo
                self.env['internal.transfer.line'].create(order_line_vals)
                # Update powerone table
                query = 'UPDATE internal_transfer SET odoo_id=%s WHERE id=%s;' % (internal_transfer.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                internal_transfer.button_confirm()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE internal_transfer SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import internal.transfer : %s' % str(e))
        return True

    @api.multi
    def import_delivery_order(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='stock_picking';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM stock_picking WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')] or 'TEST'
                if line[columns.index('partner_id')]:
                    partner_id = self.env['res.partner'].search([('name', '=', line[columns.index('partner_id')])], limit=1)
                    if not partner_id:
                        partner_id = self.env['res.partner'].create({'name': line[columns.index('partner_id')]})
                    vals['partner_id'] = partner_id.id
                vals['move_type'] = line[columns.index('move_type')]
                vals['origin'] = line[columns.index('origin')] or ' '
                vals['date_done'] = line[columns.index('date_done')]
                vals['priority'] = line[columns.index('priority')]
                if line[columns.index('location_id')]:
                    location_id = self.env['stock.location'].search([('name', 'ilike', line[columns.index('location_id')])], limit=1)
                    if not location_id:
                        location_id = self.env['stock.location'].create({'name': line[columns.index('location_id')], 'usage': 'inventory'})
                    vals['location_id'] = location_id.id
                if line[columns.index('picking_type_id')]:
                    picking_type_id = self.env['stock.picking.type'].search([('name', 'ilike', line[columns.index('picking_type_id')])], limit=1)
                    if not picking_type_id:
                        picking_type_id = self.env['stock.picking.type'].create({'name': line[columns.index('picking_type_id')]})
                    vals['picking_type_id'] = picking_type_id.id
                if line[columns.index('owner_id')]:
                    owner_id = self.env['res.partner'].search([('name', 'ilike', line[columns.index('owner_id')])], limit=1)
                    if not owner_id:
                        owner_id = self.env['res.partner'].create({'name': line[columns.index('owner_id')]})
                    vals['owner_id'] = owner_id.id
                if line[columns.index('location_dest_id')]:
                    location_dest_id = self.env['stock.location'].search([('name', 'ilike', line[columns.index('location_dest_id')])], limit=1)
                    if not location_dest_id:
                        location_dest_id = self.env['stock.location'].create({'name': line[columns.index('location_dest_id')], 'usage': 'internal'})
                    vals['location_dest_id'] = location_dest_id.id
                vals['state'] = 'draft'
                # Create stock_picking in odoo
                stock_picking = self.env['stock.picking'].create(vals)

                order_line_vals = {}
                if line[columns.index('product_id')]:
                    product_id = self.env['product.product'].search([('name', 'ilike', line[columns.index('product_id')])],limit=1)
                    if not product_id:
                        categ_id = self.env['product.category'].search([('name', 'ilike', 'All')], limit=1)
                        product_id = self.env['product.product'].create({'name': line[columns.index('product_id')], 'categ_id': categ_id.id})
                    order_line_vals['product_id'] = product_id.id
                    order_line_vals['product_uom'] = product_id.uom_id.id
                order_line_vals['product_uom_qty'] = line[columns.index('product_uom_qty')]
                order_line_vals['procure_method'] = 'make_to_stock'
                order_line_vals['name'] = line[columns.index('name')] or 'TEST'
                if line[columns.index('warehouse_id')]:
                    warehouse_id = self.env['stock.warehouse'].search([('name', 'ilike', line[columns.index('warehouse_id')])], limit=1)
                    if not warehouse_id:
                        warehouse_id = self.env['stock.warehouse'].create({'name': line[columns.index('warehouse_id')], 'code': line[columns.index('warehouse_id')]})
                    order_line_vals['warehouse_id'] = warehouse_id.id
                if line[columns.index('location_dest_id')]:
                    location_dest_id = self.env['stock.location'].search([('name', 'ilike', line[columns.index('location_dest_id')])], limit=1)
                    if not location_dest_id:
                        location_dest_id = self.env['stock.location'].create({'name': line[columns.index('location_dest_id')], 'usage': 'internal'})
                    order_line_vals['location_dest_id'] = location_dest_id.id
                if line[columns.index('location_id')]:
                    location_id = self.env['stock.location'].search([('name', 'ilike', line[columns.index('location_id')])], limit=1)
                    if not location_id:
                        location_id = self.env['stock.location'].create({'name': line[columns.index('location_id')], 'usage': 'internal'})
                    order_line_vals['location_id'] = location_id.id
                order_line_vals['picking_id'] = stock_picking.id
                order_line_vals['state'] = 'draft'
                # Create stock_inventory_line in odoo
                self.env['stock.move'].create(order_line_vals)
                # Update powerone table
                query = 'UPDATE stock_picking SET odoo_id=%s WHERE id=%s;' % (stock_picking.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                stock_picking.action_confirm()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE stock_picking SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import stock.picking : %s' % str(e))
        return True

    @api.multi
    def import_hr_expense(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='hr_expense';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        # Process data
        query = "SELECT * FROM hr_expense WHERE odoo_id is NULL LIMIT 300;"
        cr.execute(query)
        for line in cr.fetchall():
            try:
                vals = {}
                vals['name'] = line[columns.index('name')]
                vals['total_amount'] = line[columns.index('total_amount')]
                vals['date'] = line[columns.index('date_id')]
                if line[columns.index('analytic_account_id')]:
                    analytic_account_id = self.env['account.analytic.account'].search([('name', 'ilike', line[columns.index('analytic_account_id')])], limit=1)
                    if not analytic_account_id:
                        analytic_account_id = self.env['account.analytic.account'].create({'name': line[columns.index('analytic_account_id')]})
                    vals['analytic_account_id'] = analytic_account_id.id
                vals['untaxed_amount'] = line[columns.index('untaxed_amount')]
                vals['reference'] = line[columns.index('reference')]
                vals['payment_mode'] = line[columns.index('payment_mode')]
                if line[columns.index('product_id')]:
                    product_id = self.env['product.product'].search([('name', 'ilike', line[columns.index('product_id')])], limit=1)
                    categ_id = self.env['product.category'].search([('name', 'ilike', 'All')], limit=1)
                    if not product_id:
                        product_id = self.env['product.product'].create({'name': line[columns.index('product_id')], 'categ_id': categ_id.id})
                    vals['product_id'] = product_id.id
                vals['quantity'] = line[columns.index('quantity')]
                vals['unit_amount'] = line[columns.index('unit_amount')]
                vals['state'] = 'draft'
                hr_expense = self.env['hr.expense'].create(vals)
                hr_expense.submit_expenses()
                # Update powerone table
                query = 'UPDATE hr_expense SET odoo_id=%s WHERE id=%s;' % (hr_expense.id, line[columns.index('id')])
                cr.execute(query)
                conn.commit()
            except Exception, e:
                # Setting odoo_id as 0 to skip it permanently
                query = 'UPDATE hr_expense SET odoo_id=0 WHERE id=%s;' % (line[columns.index('id')])
                cr.execute(query)
                conn.commit()
                _logger.error('\nPowerone sync import hr.expense : %s' % str(e))
        return True

    @api.multi
    def import_inventory_adjustment(self, cr, conn):
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='stock_inventory';"
        cr.execute(query)
        columns = map(lambda x: x[0], cr.fetchall())
        inventory_adjustment_ids = self.env['stock.inventory'].search([('is_imported','=', False)])
        for inventory_adjustment_id in inventory_adjustment_ids:
            try:
                name = inventory_adjustment_id.name
                location_id = inventory_adjustment_id.location_id.name if inventory_adjustment_id.location_id else False
                filter = inventory_adjustment_id.filter
                category_id = inventory_adjustment_id.category_id.name if inventory_adjustment_id.category_id else False
                product_id = inventory_adjustment_id.product_id.name if inventory_adjustment_id.product_id else False
                journal_id = inventory_adjustment_id.journal_id.name if inventory_adjustment_id.journal_id else False
                accounting_date = inventory_adjustment_id.accounting_date or 'null'
                state = inventory_adjustment_id.state
                date = inventory_adjustment_id.date
                exhausted = inventory_adjustment_id.exhausted or False
                new_price = inventory_adjustment_id.new_price or False
                company_id = inventory_adjustment_id.company_id.name or False
                package_id = inventory_adjustment_id.package_id.name or False
                lot_id = inventory_adjustment_id.lot_id.name or False
                partner_id = inventory_adjustment_id.partner_id.name or False
                odoo_id = inventory_adjustment_id.id
                query = "INSERT INTO inventory_adjustment (name, location_id, filter, category_id, product_id, state, date, exhausted, new_price, journal_id, company_id, package_id, lot_id, partner_id, odoo_id) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s);" % (name, location_id, filter, category_id, product_id, state, date, exhausted, new_price, journal_id, company_id, package_id, lot_id, partner_id, odoo_id)
                cr.execute(query)
                conn.commit()
                for line in inventory_adjustment_id.line_ids:
                    theoretical_qty = line.theoretical_qty
                    product_qty = line.product_qty or False
                    product_code = line.product_code or False
                    product_name = line.product_name or False
                    prodlot_name = line.prodlot_name or False
                    package_id = line.package_id.name or False
                    location_name = line.location_name or False
                    company_id = line.company_id.name or False
                    location_id = line.location_id.name or False
                    inventory_id = line.inventory_id.name or False
                    product_id = line.product_id.name or False
                    product_uom_id = line.product_uom_id.name or False
                    prod_lot_id = line.prod_lot_id.name or False
                    unit_price = line.unit_price or 0.0
                    adjustment_column = line.adjustment_column.id or 0.0
                    change = abs(line.change) or 0.0
                    odoo_id = line.id
                    query = "INSERT INTO stock_inventory_line (theoretical_qty, product_qty, product_code, product_name, prodlot_name, package_id, location_name, company_id, location_id, inventory_id, product_id, product_uom_id, unit_price, change, odoo_id) VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s);" % (
                    theoretical_qty, product_qty, product_code, product_name, prodlot_name, package_id, location_name, company_id, location_id, inventory_id, product_id, product_uom_id, unit_price, change, odoo_id)
                    cr.execute(query)
                    conn.commit()
                inventory_adjustment_id.write({'is_imported': True})
            except Exception, e:
                _logger.error('\nPowerone sync import stock.inventory : %s' % str(e))
        return True

SyncSettings()
