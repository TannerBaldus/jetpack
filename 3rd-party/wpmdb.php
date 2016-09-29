<?php
/**
 * Compatibility file for WP Migrate DB Pro.
 *
 * @package Jetpack
 */

/**
 *
 * Block migration of the 'jetpack_options' value.
 *
 * @param array $options Array of preserved options.
 *
 * @return array $options Array of preserved options.
 */
function wpmdb_jetpack_preserved_options( $options ) {
	$options[] = 'jetpack_options';
	return $options;
}
add_filter( 'wpmdb_preserved_options', 'wpmdb_jetpack_preserved_options' );

/**
 * Activate Jetpack's Development mode when pulling from a live site.
 *
 * @param string $migration_type Migration type. push or pull.
 * @param string $connection_url URL of the site where a migration is applied.
 */
function wpmdb_jetpack_enable_dev_mode( $migration_type, $connection_url ) {

	// Check if we're on a remote site or a local one.
	global $wp_current_filter;
	if ( strstr( $wp_current_filter[0], 'nopriv' ) {
		$is_remote = true;
	} else {
		$is_remote = false;
	}

	// Only enable Development mode when pulling from a live site.
	if ( 'pull' == $action && ! $is_remote ) {
		if ( ! defined( 'JETPACK_DEV_DEBUG' ) ) {
			define( 'JETPACK_DEV_DEBUG' , true );
		}
	}
}
add_action( 'wpmdb_migration_complete', 'wpmdb_jetpack_enable_dev_mode', 10, 2 );
