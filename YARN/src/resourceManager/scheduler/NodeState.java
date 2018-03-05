package resourceManager.scheduler;

/**
 * <p>State of a <code>Node</code>.</p>
 */
public enum NodeState {
	/** New node */
	NEW, 

	/** Running node */
	RUNNING, 

	/** Node is unhealthy */
	UNHEALTHY, 

	/** Node is out of service */
	DECOMMISSIONED, 

	/** Node has not sent a heartbeat for some configured time threshold*/
	LOST, 

	/** Node has rebooted */
	REBOOTED;

	public boolean isUnusable() {
		return (this == UNHEALTHY || this == DECOMMISSIONED || this == LOST);
	}
}
