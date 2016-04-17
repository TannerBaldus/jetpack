<?php

/** 
 * A buffer of items from the queue that can be checked out
 */
class Jetpack_Sync_Queue_Buffer {
	public $id;
	public $items_with_ids;
	
	public function __construct( $items_with_ids ) {
		$this->id = uniqid();
		$this->items_with_ids = $items_with_ids;
	}

	public function get_items() {
		return array_map( function( $item ) { return $item->value; }, $this->items_with_ids );
	}

	public function get_item_ids() {
		return array_map( function( $item ) { return $item->id; }, $this->items_with_ids );
	}
}

/**
 * A persistent queue that can be flushed in increments of N items,
 * and which blocks reads until checked-out buffers are checked in or
 * closed. This uses raw SQL for two reasons: speed, and not triggering 
 * tons of added_option callbacks.
 */
class Jetpack_Sync_Queue {
	public $id;
	private $checkout_size;

	function __construct( $id, $checkout_size = 10 ) {
		$this->id = str_replace( '-', '_', $id); // necessary to ensure we don't have ID collisions in the SQL
		$this->checkout_size = $checkout_size;
	}

	function add( $item ) {
		global $wpdb;
		$added = false;
		// this basically tries to add the option until enough time has elapsed that
		// it has a unique (microtime-based) option key
		while(!$added) {
			$rows_added = $wpdb->query( $wpdb->prepare( 
				"INSERT INTO $wpdb->options (option_name, option_value) VALUES (%s, %s)", 
				$this->get_option_name(), 
				serialize($item)
			) );
			$added = ( $rows_added !== 0 );
		}
	}

	// Attempts to insert all the items in a single SQL query. May be subject to query size limits!
	function add_all( $items ) {
		global $wpdb;
		$base_option_name = $this->get_option_name();

		$query = "INSERT INTO $wpdb->options (option_name, option_value) VALUES ";
		
		$rows = array();

		for ( $i=0; $i < count( $items ); $i += 1 ) {
			$option_name = esc_sql( $base_option_name.'-'.$i );
			$option_value = esc_sql( serialize( $items[ $i ] ) );
			$rows[] = "('$option_name', '$option_value')";
		}

		$rows_added = $wpdb->query( $query . join( ',', $rows ) );

		if ( $rows_added !== count( $items ) ) {
			return new WP_Error( 'row_count_mismatch', "The number of rows inserted didn't match the size of the input array" );
		}
	}

	function reset() {
		global $wpdb;
		$this->delete_checkout_id();
		$wpdb->query( $wpdb->prepare( 
			"DELETE FROM $wpdb->options WHERE option_name LIKE %s", "jetpack_sync_queue_{$this->id}-%" 
		) );
	}

	function size() {
		global $wpdb;
		return $wpdb->get_var( $wpdb->prepare( 
			"SELECT count(*) FROM $wpdb->options WHERE option_name LIKE %s", "jetpack_sync_queue_{$this->id}-%" 
		) );
	}

	function checkout() {
		if ( $this->get_checkout_id() ) {
			return new WP_Error( 'unclosed_buffer', 'There is an unclosed buffer' );
		}

		$items = $this->fetch_items( $this->checkout_size );
		
		if ( count( $items ) === 0 ) {
			return false;
		}

		$buffer = new Jetpack_Sync_Queue_Buffer( array_slice( $items, 0, $this->checkout_size ) );
		
		$result = $this->set_checkout_id( $buffer->id );

		if ( !$result || is_wp_error( $result ) ) {
			return $result;
		}
		
		return $buffer;
	}

	function checkin( $buffer ) {
		$is_valid = $this->validate_checkout( $buffer );

		if ( is_wp_error( $is_valid ) ) {
			return $is_valid;
		}

		$this->delete_checkout_id();

		return true;
	}

	function close( $buffer ) {
		$is_valid = $this->validate_checkout( $buffer );

		if ( is_wp_error( $is_valid ) ) {
			return $is_valid;
		}

		$this->delete_checkout_id();

		global $wpdb;

		// all this fanciness is basically so we can prepare a statement with an IN(id1, id2, id3) clause
		$ids_to_remove = $buffer->get_item_ids();
		if ( count( $ids_to_remove ) > 0 ) {
			$sql = "DELETE FROM $wpdb->options WHERE option_name IN (".implode(', ', array_fill(0, count($ids_to_remove), '%s')).')';
			$query = call_user_func_array( array( $wpdb, 'prepare' ), array_merge( array( $sql ), $ids_to_remove ) );
			$wpdb->query( $query );
		}

		return true;
	}

	function flush_all() {
		$items = array_map( function( $item ) { return $item->value; }, $this->fetch_items() );
		$this->reset();
		return $items;
	}

	function get_all() {
		return $this->fetch_items();
	}

	function set_checkout_size( $new_size ) {
		$this->checkout_size = $new_size;
	}

	private function get_checkout_id() {
		return get_option( "jetpack_sync_queue_{$this->id}-checkout", false );
	}

	private function set_checkout_id( $checkout_id ) {
		$added = add_option( "jetpack_sync_queue_{$this->id}-checkout", $checkout_id, null, true ); // this one we should autoload
		if ( ! $added )
			return new WP_Error( 'buffer_mismatch', 'Another buffer is already checked out: '.$this->get_checkout_id() );
		else
			return true;
	}

	private function delete_checkout_id() {
		delete_option( "jetpack_sync_queue_{$this->id}-checkout" );
	}

	private function get_option_name() {
		// this option is specifically chosen to, as much as possible, preserve time order
		// and minimise the possibility of collisions between multiple processes working 
		// at the same time
		// TODO: confirm we only need to support PHP5 (otherwise we'll need to emulate microtime as float)
		// @see: http://php.net/manual/en/function.microtime.php
		$timestamp = sprintf( '%.9f', microtime(true) );
		return 'jetpack_sync_queue_'.$this->id.'-'.$timestamp.'-'.getmypid();
	}

	private function fetch_items( $limit = null ) {
		global $wpdb;

		if ( $limit ) {
			$query_sql = $wpdb->prepare( "SELECT option_name AS id, option_value AS value FROM $wpdb->options WHERE option_name LIKE %s ORDER BY option_name ASC LIMIT %d", "jetpack_sync_queue_{$this->id}-%", $limit );
		} else {
			$query_sql = $wpdb->prepare( "SELECT option_name AS id, option_value AS value FROM $wpdb->options WHERE option_name LIKE %s ORDER BY option_name ASC", "jetpack_sync_queue_{$this->id}-%" );
		}

		$items = $wpdb->get_results( $query_sql );
		foreach( $items as $item ) {
			$item->value = maybe_unserialize( $item->value );
		} 

		return $items;
	}

	private function validate_checkout( $buffer ) {
		if ( ! $buffer instanceof Jetpack_Sync_Queue_Buffer ) {
			return new WP_Error( 'not_a_buffer', 'You must checkin an instance of Jetpack_Sync_Queue_Buffer' );
		}

		$checkout_id = $this->get_checkout_id();

		if ( !$checkout_id ) {
			return new WP_Error( 'buffer_not_checked_out', 'There are no checked out buffers' );
		}

		if ( $checkout_id != $buffer->id ) {
			return new WP_Error( 'buffer_mismatch', 'The buffer you checked in was not checked out' );
		}

		return true;
	}
}