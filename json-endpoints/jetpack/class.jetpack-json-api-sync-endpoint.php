<?php

// POST /sites/%s/sync
class Jetpack_JSON_API_Sync_Endpoint extends Jetpack_JSON_API_Endpoint {
	protected $needed_capabilities = 'manage_options';

	protected function validate_call( $_blog_id, $capability, $check_manage_active = true ) {
		parent::validate_call( $_blog_id, $capability, false );
	}

	protected function result() {
		$args = $this->input();

		$modules = null;

		// convert list of modules in comma-delimited format into an array
		// of "$modulename => true"
		if ( isset( $args['modules'] ) && ! empty( $args['modules'] ) ) {
			$modules = array_map( '__return_true', array_flip( array_map( 'trim', explode( ',', $args['modules'] ) ) ) );
		}

		foreach ( array( 'posts', 'comments', 'users' ) as $module_name ) {
			if ( 'users' === $module_name && isset( $args[ $module_name ] ) && 'initial' === $args[ $module_name ] ) {
				$modules[ 'users' ] = 'initial';
			} elseif ( isset( $args[ $module_name ] ) ) {
				$ids = explode( ',', $args[ $module_name ] );
				if ( count( $ids ) > 0 ) {
					$modules[ $module_name ] = $ids;
				}
			}
		}

		if ( empty( $modules ) ) {
			$modules = null;
		}

		return array( 'scheduled' => Jetpack_Sync_Actions::schedule_full_sync( $modules ) );
	}

	protected function validate_queue( $query ) {
		if ( ! isset( $query ) ) {
			return new WP_Error( 'invalid_queue', 'Queue name is required', 400 );
		}

		if ( ! in_array( $query, array( 'sync', 'full_sync' ) ) ) {
			return new WP_Error( 'invalid_queue', 'Queue name should be sync or full_sync', 400 );
		}
		return $query;
	}
}

// GET /sites/%s/sync/status
class Jetpack_JSON_API_Sync_Status_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-modules.php';
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-sender.php';

		$sync_module = Jetpack_Sync_Modules::get_module( 'full-sync' );
		$sender      = Jetpack_Sync_Sender::get_instance();
		$queue       = $sender->get_sync_queue();
		$full_queue  = $sender->get_full_sync_queue();

		return array_merge(
			$sync_module->get_status(),
			array(
				'is_scheduled'          => Jetpack_Sync_Actions::is_scheduled_full_sync(),
				'queue_size'            => $queue->size(),
				'queue_lag'             => $queue->lag(),
				'queue_next_sync'       => ( $sender->get_next_sync_time( 'sync' ) - microtime( true ) ),
				'full_queue_size'       => $full_queue->size(),
				'full_queue_lag'        => $full_queue->lag(),
				'full_queue_next_sync'  => ( $sender->get_next_sync_time( 'full_sync' ) - microtime( true ) ),
			)
		);
	}
}

// GET /sites/%s/data-check
class Jetpack_JSON_API_Sync_Check_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-sender.php';

		$sender     = Jetpack_Sync_Sender::get_instance();
		$sync_queue = $sender->get_sync_queue();

		// lock sending from the queue while we compare checksums with the server
		$result = $sync_queue->lock( 30 ); // tries to acquire the lock for up to 30 seconds

		if ( ! $result ) {
			$sync_queue->unlock();

			return new WP_Error( 'unknown_error', 'Unknown error trying to lock the sync queue' );
		}

		if ( is_wp_error( $result ) ) {
			$sync_queue->unlock();

			return $result;
		}

		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-wp-replicastore.php';

		$store = new Jetpack_Sync_WP_Replicastore();

		$result = $store->checksum_all();

		$sync_queue->unlock();

		return $result;

	}
}

// GET /sites/%s/data-histogram
class Jetpack_JSON_API_Sync_Histogram_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-sender.php';

		$sender     = Jetpack_Sync_Sender::get_instance();
		$sync_queue = $sender->get_sync_queue();

		// lock sending from the queue while we compare checksums with the server
		$result = $sync_queue->lock( 30 ); // tries to acquire the lock for up to 30 seconds

		if ( ! $result ) {
			$sync_queue->unlock();

			return new WP_Error( 'unknown_error', 'Unknown error trying to lock the sync queue' );
		}

		if ( is_wp_error( $result ) ) {
			$sync_queue->unlock();

			return $result;
		}

		$args = $this->query_args();

		if ( isset( $args['columns'] ) ) {
			$columns = array_map( 'trim', explode( ',', $args['columns'] ) );
		} else {
			$columns = null; // go with defaults
		}

		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-wp-replicastore.php';

		$store = new Jetpack_Sync_WP_Replicastore();

		$result = $store->checksum_histogram( $args['object_type'], $args['buckets'], $args['start_id'], $args['end_id'], $columns );

		$sync_queue->unlock();

		return $result;

	}
}

// POST /sites/%s/sync/settings
class Jetpack_JSON_API_Sync_Modify_Settings_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		$args = $this->input();

		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-settings.php';

		$sync_settings = Jetpack_Sync_Settings::get_settings();

		foreach ( $args as $key => $value ) {
			if ( $value !== false ) {
				if ( is_numeric( $value ) ) {
					$value = (int) $value;
				}
				
				// special case for sending empty arrays - a string with value 'empty'
				if ( $value === 'empty' ) {
					$value = array();
				}

				$sync_settings[ $key ] = $value;
			}
		}

		Jetpack_Sync_Settings::update_settings( $sync_settings );

		// re-fetch so we see what's really being stored
		return Jetpack_Sync_Settings::get_settings();
	}
}

// GET /sites/%s/sync/settings
class Jetpack_JSON_API_Sync_Get_Settings_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-settings.php';

		return Jetpack_Sync_Settings::get_settings();
	}
}

// GET /sites/%s/sync/object
class Jetpack_JSON_API_Sync_Object extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		$args = $this->query_args();

		$module_name = $args['module_name'];

		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-modules.php';

		if ( ! $sync_module = Jetpack_Sync_Modules::get_module( $module_name ) ) {
			return new WP_Error( 'invalid_module', 'You specified an invalid sync module' );
		}

		$object_type = $args['object_type'];
		$object_ids  = $args['object_ids'];

		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-sender.php';
		$codec = Jetpack_Sync_Sender::get_instance()->get_codec();

		return array(
			'objects' => $codec->encode( $sync_module->get_objects_by_id( $object_type, $object_ids ) )
		);
	}
}

class Jetpack_JSON_API_Sync_Now_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		$args = $this->input();
		$queue_name = $this->validate_queue( $args['queue'] );

		if ( is_wp_error( $queue_name ) ){
			return $queue_name;
		}

		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-sender.php';

		$sender = Jetpack_Sync_Sender::get_instance();
		$response = $sender->do_sync_for_queue( new Jetpack_Sync_Queue( $queue_name ) );

		return array(
			'response' => $response
		);
	}
}

class Jetpack_JSON_API_Sync_Checkout_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		$args = $this->query_args();
		$queue_name = $this->validate_queue( $args['queue'] );

		if ( is_wp_error( $queue_name ) ){
			return $queue_name;
		}

		if ( is_int( $args[ 'number_of_items' ] ) && (int) $args[ 'number_of_items' ] < 1 ) {
			return  new WP_Error( 'invalid_number_of_items', 'Number of items needs to be an integer that is larger than 0', 400 );
		}
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-sender.php';
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-queue.php';
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-json-deflate-array-codec.php';

		$queue = new Jetpack_Sync_Queue( $queue_name );
		$codec = new Jetpack_Sync_JSON_Deflate_Array_Codec();

		// We need the sender to set up all the before send module actions...
		$sender = Jetpack_Sync_Sender::get_instance();
		
		$skipped_items_ids = array();

		// let's delete the checkin state
		if ( isset( $args['force'] ) && $args['force'] ) {
			$queue->force_checkin();
		}
		
		$encode = ( $args['encode'] ? true : false );

		$codec_name = $encode ? $codec->name() : null;

		$buffer = $this->get_buffer( $queue, $args[ 'number_of_items' ] );
		
		// Check that the $buffer is not checkout out already
		if ( is_wp_error( $buffer ) ) {
			return new WP_Error( 'buffer_open', "We couldn't get the buffer it is currently checked out", 400 );
		}
		
		if ( ! is_object( $buffer ) ) {
			return new WP_Error( 'buffer_non-object', 'Buffer is not an object', 400 );
		}

		$items = $buffer->get_items();

		// set up current screen to avoid errors rendering content
		require_once( ABSPATH . 'wp-admin/includes/class-wp-screen.php' );
		require_once( ABSPATH . 'wp-admin/includes/screen.php' );
		set_current_screen( 'sync' );

		foreach ( $items as $key => $item ) {
			// Suspending cache addition help prevent overloading in memory cache of large sites.
			wp_suspend_cache_addition( true );

			/** This filter is documented in sync/class.jetpack-sync-sender.php */
			$item[1] = apply_filters( 'jetpack_sync_before_send_' . $item[0], $item[1], $item[2] );
			wp_suspend_cache_addition( false );
			if ( $item[1] === false ) {
				$skipped_items_ids[] = $key;
				continue;
			}
			$items_to_send[ $key ] = ( $encode ?  $codec->encode( $item ) : $item );
		}
	
		return array(
			'buffer_id' => $buffer->id,
			'items' => $items_to_send,
			'skipped_items' => $skipped_items_ids,
			'codec' => $codec_name,
			'server_microtime' => microtime( true ),
		);
	}

	protected function get_buffer( $queue, $number_of_items ) {
		$start = time();
		$max_duration = 5;

		$buffer = $this->try_getting_buffer( $queue , $number_of_items );
		$duration = time() - $start;

		while( ! $buffer && $duration < $max_duration ) {
			$buffer = $this->try_getting_buffer( $queue , $number_of_items );
			$duration = time() - $start;
		}

		return $buffer;
	}

	protected function try_getting_buffer( $queue , $number_of_items ) {

		$buffer = $queue->checkout( $number_of_items );
		if ( is_wp_error( $buffer ) ) {
			sleep( 2 ); // let's wait for 2 seconds before we even allow this function to be called again.
		}
		return $buffer;
	}
}

class Jetpack_JSON_API_Sync_Close_Endpoint extends Jetpack_JSON_API_Sync_Endpoint {
	protected function result() {
		$args = $this->query_args();
		$queue_name = $this->validate_queue( $args['queue'] );

		if( is_wp_error( $queue_name ) ) {
			return $queue_name;
		}
		require_once JETPACK__PLUGIN_DIR . 'sync/class.jetpack-sync-queue.php';

		if ( ! isset( $args['buffer_id'] ) ) {
			return new WP_Error( 'missing_buffer_id', 'Please provide a buffer id', 400 );
		}

		$request_body = $this->input();

		if ( ! isset( $request_body['item_ids'] ) && is_array( $request_body['item_ids'] ) ) {
			return new WP_Error( 'missing_item_ids', 'Please provide a list of item ids in the item_ids argument', 400 );
		}

		$buffer = new Jetpack_Sync_Queue_Buffer( $args['buffer_id'], $args['item_ids'] );
		$queue = new Jetpack_Sync_Queue( $queue_name );
		$response = $queue->close( $buffer, $args['item_ids'] );
		
		if ( is_wp_error( $response ) ) {
			return $response;
		}

		return array(
			'success' => $response
		);
	}
}