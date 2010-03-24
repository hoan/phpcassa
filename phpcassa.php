<?php

// Setting up nodes:
//
// CassandraConn::add_node('192.168.1.1', 9160);
// CassandraConn::add_node('192.168.1.2', 5000);
//

// Querying:
//
// $users = new CassandraCF('Keyspace1', 'Users');
// $users->insert('1', array('email' => 'hoan.tonthat@gmail.com', 'password' => 'test'));
// $users->get('1');
// $users->multiget(array(1, 2));
// $users->get_count('1');
// $users->get_range('1', '10');
// $users->remove('1');
// $users->remove('1', 'password');
//

class CassandraConn {
    const DEFAULT_THRIFT_PORT = 9160;
    
    static private $connections = array();
    static private $last_error;

    static public function add_node($host, $port=self::DEFAULT_THRIFT_PORT) {
        try {
            // Create Thrift transport and binary protocol cassandra client
            $transport = new TBufferedTransport(new TSocket($host, $port), 1024, 1024);
            $client    = new CassandraClient(new TBinaryProtocolAccelerated($transport));

            // Store it in the connections
            self::$connections[] = array(
                'transport' => $transport,
                'client'    => $client
            );

            // Done
            return TRUE;
        } catch (TException $tx) {
            self::$last_error = 'TException: '.$tx->getMessage() . "\n";
        }
        return FALSE;
    }
    
    // Default client
    static public function get_client($write_mode = false) {
        // * Try to connect to every cassandra node in order
        // * Failed connections will be retried
        // * Once a connection is opened, it stays open
        // * TODO: add random and round robin order
        // * TODO: add write-preferred and read-preferred nodes
        foreach(self::$connections as $connection) {
            try {
                $transport = $connection['transport'];
                $client    = $connection['client'];

                if(!$transport->isOpen()) {
                    $transport->open();
                }

                return $client;
            } catch (TException $tx) {
                self::$last_error = 'TException: '.$tx->getMessage() . "\n";
                continue;
            }
        }

        throw new Exception("Could not connect to a cassandra server");
    }
}

class CassandraCF {
    const DEFAULT_ROW_LIMIT = 100; // default max # of rows for get_range()
    const DEFAULT_COLUMN_TYPE = "UTF8String";
    const DEFAULT_SUBCOLUMN_TYPE = null;
    
    public $keyspace;
    public $column_family;
    public $is_super;
    public $read_consistency_level;
    public $write_consistency_level;
    public $column_type; // CompareWith (TODO: actually use this)
    public $subcolumn_type; // CompareSubcolumnsWith (TODO: actually use this)
    public $parse_columns;
    
    /*
    BytesType: Simple sort by byte value. No validation is performed.
    AsciiType: Like BytesType, but validates that the input can be parsed as US-ASCII.
    UTF8Type: A string encoded as UTF8
    LongType: A 64bit long
    LexicalUUIDType: A 128bit UUID, compared lexically (by byte value)
    TimeUUIDType: a 128bit version 1 UUID, compared by timestamp
    */
    
    public function __construct($keyspace, $column_family,
                                $is_super=false,
                                $column_type=self::DEFAULT_COLUMN_TYPE,
                                $subcolumn_type=self::DEFAULT_SUBCOLUMN_TYPE,
                                $read_consistency_level=cassandra_ConsistencyLevel::ONE,
                                $write_consistency_level=cassandra_ConsistencyLevel::ZERO) {
        // Vars
        $this->keyspace = $keyspace;
        $this->column_family = $column_family;

        $this->is_super = $is_super;

        $this->column_type = $column_type;
        $this->subcolumn_type = $subcolumn_type;

        $this->read_consistency_level = $read_consistency_level;
        $this->write_consistency_level = $write_consistency_level;

        // Toggles parsing columns
        $this->parse_columns = true;
    }

    public function get($key, $super_column=NULL, $slice_start="", $slice_finish="") {
        $column_parent = new cassandra_ColumnParent();
        $column_parent->column_family = $this->column_family;
        $column_parent->super_column = $this->unparse_column_name($super_column, false);

        $slice_range = new cassandra_SliceRange();
        $slice_range->start = $slice_start;
        $slice_range->finish = $slice_finish;
        $predicate = new cassandra_SlicePredicate();
        $predicate->slice_range = $slice_range;

        $client = CassandraConn::get_client();
        $resp = $client->get_slice($this->keyspace, $key, $column_parent, $predicate, $this->read_consistency_level);

        return $this->supercolumns_or_columns_to_array($resp);
    }

    public function multiget($keys, $slice_start="", $slice_finish="") {
        $column_parent = new cassandra_ColumnParent();
        $column_parent->column_family = $this->column_family;
        $column_parent->super_column = NULL;

        $slice_range = new cassandra_SliceRange();
        $slice_range->start = $slice_start;
        $slice_range->finish = $slice_finish;
        $predicate = new cassandra_SlicePredicate();
        $predicate->slice_range = $slice_range;

        $client = CassandraConn::get_client();
        $resp = $client->multiget_slice($this->keyspace, $keys, $column_parent, $predicate, $this->read_consistency_level);

        $ret = null;
        foreach($resp as $name => $value) {
            $ret[$name] = $this->supercolumns_or_columns_to_array($value);
        }
        return $ret;
    }

    public function get_count($key, $super_column=null) {
        $column_path = new cassandra_ColumnPath();
        $column_path->column_family = $this->column_family;
        $column_path->super_column = $super_column;

        $client = CassandraConn::get_client();
        $resp = $client->get_count($this->keyspace, $key, $column_path, $this->read_consistency_level);

        return $resp;
    }

    public function get_range($start_key="", $finish_key="", $row_count=self::DEFAULT_ROW_LIMIT, $slice_start="", $slice_finish="") {
        $column_parent = new cassandra_ColumnParent();
        $column_parent->column_family = $this->column_family;
        $column_parent->super_column = NULL;

        $slice_range = new cassandra_SliceRange();
        $slice_range->start = $slice_start;
        $slice_range->finish = $slice_finish;
        $predicate = new cassandra_SlicePredicate();
        $predicate->slice_range = $slice_range;

        $client = CassandraConn::get_client();
        $resp = $client->get_range_slice($this->keyspace, $column_parent, $predicate,
                                         $start_key, $finish_key, $row_count,
                                         $this->read_consistency_level);

        return $this->keyslices_to_array($resp);
    }

    public function insert($key, $columns) {
        $timestamp = time();

        $cfmap = array();
        $cfmap[$this->column_family] = $this->array_to_supercolumns_or_columns($columns, $timestamp);

        $client = CassandraConn::get_client();
        $resp = $client->batch_insert($this->keyspace, $key, $cfmap, $this->write_consistency_level);

        return $resp;
    }

    public function remove($key, $column_name=null) {
        $timestamp = time();

        $column_path = new cassandra_ColumnPath();
        $column_path->column_family = $this->column_family;
        if($this->is_super) {
            $column_path->super_column = $this->unparse_column_name($column_name, true);
        } else {
            $column_path->column = $this->unparse_column_name($column_name, false);
        }

        $client = CassandraConn::get_client();
        $resp = $client->remove($this->keyspace, $key, $column_path, $timestamp, $this->write_consistency_level);

        return $resp;
    }
    
    // Wrappers
    public function get_list($key, $key_name='key', $slice_start="", $slice_finish="") {
        // Must be on supercols!
        $resp = $this->get($key, NULL, $slice_start, $slice_finish);
        $ret = array();
        foreach($resp as $_key => $_value) {
            $_value[$key_name] = $_key;
            $ret[] = $_value;
        }
        return $ret;
    }
    
    public function get_range_list($key_name='key', $start_key="", $finish_key="",
                                   $row_count=self::DEFAULT_ROW_LIMIT, $slice_start="", $slice_finish="") {
        $resp = $this->get_range($start_key, $finish_key, $row_count, $slice_start, $slice_finish);
        $ret = array();
        foreach($resp as $_key => $_value) {
            if(!empty($_value)) { // filter nulls
                $_value[$key_name] = $_key;
                $ret[] = $_value;
            }
        }
        return $ret;
    }
    
    // Helpers for parsing Cassandra's thrift objects into PHP arrays
    public function keyslices_to_array($keyslices) {
        $ret = null;
        foreach($keyslices as $keyslice) {
            $key     = $keyslice->key;
            $columns = $keyslice->columns;

            $ret[$key] = $this->supercolumns_or_columns_to_array($columns);
        }
        return $ret;
    }

    public function supercolumns_or_columns_to_array($array_of_c_or_sc) {
        $ret = null;
        foreach($array_of_c_or_sc as $c_or_sc) {
            if($c_or_sc->column) { // normal columns
                $name  = $this->parse_column_name($c_or_sc->column->name, true);
                $value = $c_or_sc->column->value;

                $ret[$name] = $value;
            } else if($c_or_sc->super_column) { // super columns
                $name    = $this->parse_column_name($c_or_sc->super_column->name, true);
                $columns = $c_or_sc->super_column->columns;

                $ret[$name] = $this->columns_to_array($columns);
            }
        }
        return $ret;
    }

    public function columns_to_array($array_of_c) {
        $ret = null;
        foreach($array_of_c as $c) {
            $name  = $this->parse_column_name($c->name, false);
            $value = $c->value;

            $ret[$name] = $value;
        }
        return $ret;
    }

    // Helpers for turning PHP arrays into Cassandra's thrift objects
    public function array_to_supercolumns_or_columns($array, $timestamp=null) {
        if(empty($timestamp)) $timestamp = time();

        $ret = null;
        foreach($array as $name => $value) {
            $c_or_sc = new cassandra_ColumnOrSuperColumn();
            if(is_array($value)) {
                $c_or_sc->super_column = new cassandra_SuperColumn();
                $c_or_sc->super_column->name = $this->unparse_column_name($name, true);
                $c_or_sc->super_column->columns = $this->array_to_columns($value, $timestamp);
                $c_or_sc->super_column->timestamp = $timestamp;
            } else {
                $c_or_sc = new cassandra_ColumnOrSuperColumn();
                $c_or_sc->column = new cassandra_Column();
                $c_or_sc->column->name = $this->unparse_column_name($name, true);
                $c_or_sc->column->value = $value;
                $c_or_sc->column->timestamp = $timestamp;
            }
            $ret[] = $c_or_sc;
        }

        return $ret;
    }

    public function array_to_columns($array, $timestamp=null) {
        if(empty($timestamp)) $timestamp = time();

        $ret = null;
        foreach($array as $name => $value) {
            $column = new cassandra_Column();
            $column->name = $this->unparse_column_name($name, false);
            $column->value = $value;
            $column->timestamp = $timestamp;

            $ret[] = $column;
        }
        return $ret;
    }

    // ARGH
    public function parse_column_name($column_name, $is_column=true) {
        if(!$this->parse_columns) return $column_name;
        if(!$column_name) return NULL;

        $type = $is_column ? $this->column_type : $this->subcolumn_type;
        if($type == "LexicalUUIDType" || $type == "TimeUUIDType") {
            return UUID::convert($column_name, UUID::FMT_BINARY, UUID::FMT_STRING);
        } else if($type == "LongType") {
            $tmp = unpack("L", $column_name); // FIXME: currently only supports 32 bit unsigned
            return $tmp[1];
        } else {
            return $column_name;
        }
    }
    
    public function unparse_column_name($column_name, $is_column=true) {
        if(!$this->parse_columns) return $column_name;
        if(!$column_name) return NULL;

        $type = $is_column ? $this->column_type : $this->subcolumn_type;
        if($type == "LexicalUUIDType" || $type == "TimeUUIDType") {
            return UUID::convert($column_name, UUID::FMT_STRING, UUID::FMT_BINARY);
        } else if($type == "LongType") {
            return pack("LL", $column_name, 0); // FIXME: currently only supports 32 bit unsigned
        } else {
            return $column_name;
        }
    }
}

?>
