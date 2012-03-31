import re
from gluon.dal import DAL, OracleAdapter, Expression, Table, Field, Query

regex_type = re.compile('^([\w\_\:]+)')
regex_dbname = re.compile('^(\w+)(\:\w+)*')
regex_table_field = re.compile('^([\w_]+)\.([\w_]+)$')
regex_content = re.compile('(?P<table>[\w\-]+)\.(?P<field>[\w\-]+)\.(?P<uuidkey>[\w\-]+)\.(?P<name>\w+)\.\w+$')
regex_cleanup_fn = re.compile('[\'"\s;]+')
string_unpack=re.compile('(?<!\|)\|(?!\|)')
regex_python_keywords = re.compile('^(and|del|from|not|while|as|elif|global|or|with|assert|else|if|pass|yield|break|except|import|print|class|exec|in|raise|continue|finally|is|return|def|for|lambda|try)$')
regex_select_as_parser = re.compile("\s+AS\s+(\S+)")

class MyDAL(DAL):
   
    def __init__(self, uri='sqlite://dummy.db',
                 pool_size=0, folder=None,
                 db_codec='UTF-8', check_reserved=None,
                 migrate=True, fake_migrate=False,
                 migrate_enabled=True, fake_migrate_all=False,
                 decode_credentials=False, driver_args=None,
                 adapter_args=None, attempts=5, auto_import=False):
        super(MyDAL,self).__init__(uri,
                 pool_size, folder,
                 db_codec, check_reserved,
                 migrate, fake_migrate,
                 migrate_enabled, fake_migrate_all,
                 decode_credentials, driver_args,
                 adapter_args, attempts, auto_import)
        if 'Oracle' in str(type(self._adapter)):
            if not decode_credentials:
                credential_decoder = lambda cred: cred
            else:
                credential_decoder = lambda cred: urllib.unquote(cred)
            self._adapter = MyOracleAdapter(self,uri,pool_size,folder,
                                db_codec, credential_decoder,
                                driver_args or {}, adapter_args or {})
        elif 'Postgre' in str(type(self._adapter)):
            if not decode_credentials:
                credential_decoder = lambda cred: cred
            else:
                credential_decoder = lambda cred: urllib.unquote(cred)
            self._adapter = MyPostgreSQLAdapter(self,uri,pool_size,folder,
                                db_codec, credential_decoder,
                                driver_args or {}, adapter_args or {})

class MyPostgreSQLAdapter(PostgreSQLAdapter):

    def use_common_filters(self, query):
        return (query and hasattr(query,'ignore_common_filters') and \
                not query.ignore_common_filters)

    def _select(self, query, fields, attributes):
        for key in set(attributes.keys())-set(('orderby', 'groupby', 'limitby',
                                               'required', 'cache', 'left',
                                               'distinct', 'having', 'join',
                                               'for_update')):
            raise SyntaxError, 'invalid select attribute: %s' % key

        tablenames = self.tables(query)
        for field in fields:
            if isinstance(field, basestring) and regex_table_field.match(field):
                tn,fn = field.split('.')
                field = self.db[tn][fn]
            for tablename in self.tables(field):
                if not tablename in tablenames:
                    tablenames.append(tablename)

        if self.use_common_filters(query):
            query = self.common_filter(query,tablenames)

        if len(tablenames) < 1:
            raise SyntaxError, 'Set: no tables selected'
        sql_f = ', '.join(map(self.expand, fields))
        self._colnames = [c.strip() for c in sql_f.split(', ')]
        if query:
            sql_w = ' WHERE ' + self.expand(query)
        else:
            sql_w = ''
        sql_o = ''
        sql_s = ''
        left = attributes.get('left', False)
        inner_join = attributes.get('join', False)
        distinct = attributes.get('distinct', False)
        groupby = attributes.get('groupby', False)
        orderby = attributes.get('orderby', False)
        having = attributes.get('having', False)
        limitby = attributes.get('limitby', False)
        for_update = attributes.get('for_update', False)
        if self.can_select_for_update is False and for_update is True:
            raise SyntaxError, 'invalid select attribute: for_update'
        if distinct is True:
            sql_s += 'DISTINCT'
        elif distinct:
            sql_s += 'DISTINCT ON (%s)' % distinct
        if inner_join:
            icommand = self.JOIN()
            if not isinstance(inner_join, (tuple, list)):
                inner_join = [inner_join]
            ijoint = [t._tablename for t in inner_join if not isinstance(t,Expression)]
            ijoinon = [t for t in inner_join if isinstance(t, Expression)]
            itables_to_merge={} #issue 490
            [itables_to_merge.update(dict.fromkeys(self.tables(t))) for t in ijoinon] # issue 490
            ijoinont = [t.first._tablename for t in ijoinon]
            # start schema support 
            dot_filtered_tbl = []
            for tbl in ijoinont:
                dot_tbl = tbl.split('.')
                if len(dot_tbl)>1:
                    dot_filtered_tbl.append(dot_tbl[1])
                else:
                    dot_filtered_tbl.append(tbl)
            ijoinont =dot_filtered_tbl[:]
            # end schema support
            [itables_to_merge.pop(t) for t in ijoinont if t in itables_to_merge] #issue 490
            iimportant_tablenames = ijoint + ijoinont + itables_to_merge.keys() # issue 490         
            iexcluded = [t for t in tablenames if not t in iimportant_tablenames]
        if left:
            join = attributes['left']
            command = self.LEFT_JOIN()
            if not isinstance(join, (tuple, list)):
                join = [join]
            joint = [t._tablename for t in join if not isinstance(t, Expression)]
            joinon = [t for t in join if isinstance(t, Expression)]
            #patch join+left patch (solves problem with ordering in left joins)
            tables_to_merge={}
            [tables_to_merge.update(dict.fromkeys(self.tables(t))) for t in joinon]
            joinont = [t.first._tablename for t in joinon]
            # start schema support 
            dot_filtered_tbl = []
            for tbl in joinont:
                dot_tbl = tbl.split('.')
                if len(dot_tbl)>1:
                    dot_filtered_tbl.append(dot_tbl[1])
                else:
                    dot_filtered_tbl.append(tbl)
            joinont =dot_filtered_tbl[:] 
            # end schema support 
            [tables_to_merge.pop(t) for t in joinont if t in tables_to_merge]
            important_tablenames = joint + joinont + tables_to_merge.keys()
            excluded = [t for t in tablenames if not t in important_tablenames ]
        def alias(t):
            return str(self.db[t])
        if inner_join and not left:
            sql_t = ', '.join([alias(t) for t in iexcluded + itables_to_merge.keys()]) # issue 490
            for t in ijoinon:
                sql_t += ' %s %s' % (icommand, str(t))
        elif not inner_join and left:
            sql_t = ', '.join([alias(t) for t in excluded + tables_to_merge.keys()])
            if joint:
                sql_t += ' %s %s' % (command, ','.join([t for t in joint]))
            for t in joinon:
                sql_t += ' %s %s' % (command, str(t))
        elif inner_join and left:
            all_tables_in_query = set(important_tablenames + \
                                      iimportant_tablenames + \
                                      tablenames) # issue 490
            tables_in_joinon = set(joinont + ijoinont) # issue 490
            tables_not_in_joinon = all_tables_in_query.difference(tables_in_joinon) # issue 490
            sql_t = ','.join([alias(t) for t in tables_not_in_joinon]) # issue 490
            for t in ijoinon:
                sql_t += ' %s %s' % (icommand, str(t))
            if joint:
                sql_t += ' %s %s' % (command, ','.join([t for t in joint]))
            for t in joinon:
                sql_t += ' %s %s' % (command, str(t))
        else:
            sql_t = ', '.join(alias(t) for t in tablenames)
        if groupby:
            if isinstance(groupby, (list, tuple)):
                groupby = xorify(groupby)
            sql_o += ' GROUP BY %s' % self.expand(groupby)
            if having:
                sql_o += ' HAVING %s' % attributes['having']
        if orderby:
            if isinstance(orderby, (list, tuple)):
                orderby = xorify(orderby)
            if str(orderby) == '<random>':
                sql_o += ' ORDER BY %s' % self.RANDOM()
            else:
                sql_o += ' ORDER BY %s' % self.expand(orderby)
        if limitby:
            if not orderby and tablenames:
                sql_o += ' ORDER BY %s' % ', '.join(['%s.%s'%(t,x) for t in tablenames for x in ((hasattr(self.db[t], '_primarykey') and self.db[t]._primarykey) or [self.db[t]._id.name])])
            # oracle does not support limitby
        sql = self.select_limitby(sql_s, sql_f, sql_t, sql_w, sql_o, limitby)
        if for_update and self.can_select_for_update is True:
            sql = sql.rstrip(';') + ' FOR UPDATE;'
        return sql

class MyOracleAdapter(OracleAdapter):

    def use_common_filters(self, query):
        return (query and hasattr(query,'ignore_common_filters') and \
                not query.ignore_common_filters)

    def select_limitby(self, sql_s, sql_f, sql_t, sql_w, sql_o, limitby):
        alias = sql_t
        if 'JOIN' not in sql_t:
            splitted = sql_t.split('.')
            if len(splitted) > 1:
                alias = splitted[1]
        if limitby:
            (lmin, lmax) = limitby
            if len(sql_w) > 1:
                sql_w_row = sql_w + ' AND w_row > %i' % lmin
            else:
                sql_w_row = 'WHERE w_row > %i' % lmin
            return 'SELECT %s %s FROM (SELECT w_tmp.*, ROWNUM w_row FROM (SELECT %s FROM %s%s%s) w_tmp WHERE ROWNUM<=%i) %s %s %s;' % (sql_s, sql_f, sql_f, sql_t, sql_w, sql_o, lmax, alias , sql_w_row, sql_o)
        return 'SELECT %s %s FROM %s%s%s;' % (sql_s, sql_f, sql_t, sql_w, sql_o)

    def _select(self, query, fields, attributes):
        for key in set(attributes.keys())-set(('orderby', 'groupby', 'limitby',
                                               'required', 'cache', 'left',
                                               'distinct', 'having', 'join',
                                               'for_update')):
            raise SyntaxError, 'invalid select attribute: %s' % key

        tablenames = self.tables(query)
        for field in fields:
            if isinstance(field, basestring) and regex_table_field.match(field):
                tn,fn = field.split('.')
                field = self.db[tn][fn]
            for tablename in self.tables(field):
                if not tablename in tablenames:
                    tablenames.append(tablename)

        if self.use_common_filters(query):
            query = self.common_filter(query,tablenames)

        if len(tablenames) < 1:
            raise SyntaxError, 'Set: no tables selected'
        sql_f = ', '.join(map(self.expand, fields))
        self._colnames = [c.strip() for c in sql_f.split(', ')]
        if query:
            sql_w = ' WHERE ' + self.expand(query)
        else:
            sql_w = ''
        sql_o = ''
        sql_s = ''
        left = attributes.get('left', False)
        inner_join = attributes.get('join', False)
        distinct = attributes.get('distinct', False)
        groupby = attributes.get('groupby', False)
        orderby = attributes.get('orderby', False)
        having = attributes.get('having', False)
        limitby = attributes.get('limitby', False)
        for_update = attributes.get('for_update', False)
        if self.can_select_for_update is False and for_update is True:
            raise SyntaxError, 'invalid select attribute: for_update'
        if distinct is True:
            sql_s += 'DISTINCT'
        elif distinct:
            sql_s += 'DISTINCT ON (%s)' % distinct
        if inner_join:
            icommand = self.JOIN()
            if not isinstance(inner_join, (tuple, list)):
                inner_join = [inner_join]
            ijoint = [t._tablename for t in inner_join if not isinstance(t,Expression)]
            ijoinon = [t for t in inner_join if isinstance(t, Expression)]
            itables_to_merge={} #issue 490
            [itables_to_merge.update(dict.fromkeys(self.tables(t))) for t in ijoinon] # issue 490
            ijoinont = [t.first._tablename for t in ijoinon]
            # start schema support 
            dot_filtered_tbl = []
            for tbl in ijoinont:
                dot_tbl = tbl.split('.')
                if len(dot_tbl)>1:
                    dot_filtered_tbl.append(dot_tbl[1])
                else:
                    dot_filtered_tbl.append(tbl)
            ijoinont =dot_filtered_tbl[:]
            # end schema support
            [itables_to_merge.pop(t) for t in ijoinont if t in itables_to_merge] #issue 490
            iimportant_tablenames = ijoint + ijoinont + itables_to_merge.keys() # issue 490         
            iexcluded = [t for t in tablenames if not t in iimportant_tablenames]
        if left:
            join = attributes['left']
            command = self.LEFT_JOIN()
            if not isinstance(join, (tuple, list)):
                join = [join]
            joint = [t._tablename for t in join if not isinstance(t, Expression)]
            joinon = [t for t in join if isinstance(t, Expression)]
            #patch join+left patch (solves problem with ordering in left joins)
            tables_to_merge={}
            [tables_to_merge.update(dict.fromkeys(self.tables(t))) for t in joinon]
            joinont = [t.first._tablename for t in joinon]
            # start schema support 
            dot_filtered_tbl = []
            for tbl in joinont:
                dot_tbl = tbl.split('.')
                if len(dot_tbl)>1:
                    dot_filtered_tbl.append(dot_tbl[1])
                else:
                    dot_filtered_tbl.append(tbl)
            joinont =dot_filtered_tbl[:] 
            # end schema support 
            [tables_to_merge.pop(t) for t in joinont if t in tables_to_merge]
            important_tablenames = joint + joinont + tables_to_merge.keys()
            excluded = [t for t in tablenames if not t in important_tablenames ]
        def alias(t):
            return str(self.db[t])
        if inner_join and not left:
            sql_t = ', '.join([alias(t) for t in iexcluded + itables_to_merge.keys()]) # issue 490
            for t in ijoinon:
                sql_t += ' %s %s' % (icommand, str(t))
        elif not inner_join and left:
            sql_t = ', '.join([alias(t) for t in excluded + tables_to_merge.keys()])
            if joint:
                sql_t += ' %s %s' % (command, ','.join([t for t in joint]))
            for t in joinon:
                sql_t += ' %s %s' % (command, str(t))
        elif inner_join and left:
            all_tables_in_query = set(important_tablenames + \
                                      iimportant_tablenames + \
                                      tablenames) # issue 490
            tables_in_joinon = set(joinont + ijoinont) # issue 490
            tables_not_in_joinon = all_tables_in_query.difference(tables_in_joinon) # issue 490
            sql_t = ','.join([alias(t) for t in tables_not_in_joinon]) # issue 490
            for t in ijoinon:
                sql_t += ' %s %s' % (icommand, str(t))
            if joint:
                sql_t += ' %s %s' % (command, ','.join([t for t in joint]))
            for t in joinon:
                sql_t += ' %s %s' % (command, str(t))
        else:
            sql_t = ', '.join(alias(t) for t in tablenames)
        if groupby:
            if isinstance(groupby, (list, tuple)):
                groupby = xorify(groupby)
            sql_o += ' GROUP BY %s' % self.expand(groupby)
            if having:
                sql_o += ' HAVING %s' % attributes['having']
        if orderby:
            if isinstance(orderby, (list, tuple)):
                orderby = xorify(orderby)
            if str(orderby) == '<random>':
                sql_o += ' ORDER BY %s' % self.RANDOM()
            else:
                sql_o += ' ORDER BY %s' % self.expand(orderby)
        if limitby:
            if not orderby and tablenames:
                sql_o += ' ORDER BY %s' % ', '.join(['%s.%s'%(t,x) for t in tablenames for x in ((hasattr(self.db[t], '_primarykey') and self.db[t]._primarykey) or [self.db[t]._id.name])])
            # oracle does not support limitby
        sql = self.select_limitby(sql_s, sql_f, sql_t, sql_w, sql_o, limitby)
        if for_update and self.can_select_for_update is True:
            sql = sql.rstrip(';') + ' FOR UPDATE;'
        return sql

class MyTable(Table):
    _schema = ''
    _use_tablename = ''

    def set_schema(self, schema, **kwargs):
        self._schema = str(schema)
        self._use_tablename = self._tablename
        self._tablename = str(schema) + self._tablename
        self._sequence_name = str(schema) + self._sequence_name
        self._trigger_name = str(schema) + self._trigger_name
        migrate = kwargs.get('migrate', False)
        _adapter = kwargs.get('_adapter', False)
        if migrate and not _adapter:
            raise SyntaxError, 'Create table has been set but no _adapter was set'
        elif migrate and _adapter:
            _adapter.create_table(self)


