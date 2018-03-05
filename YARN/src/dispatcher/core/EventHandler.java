package dispatcher.core;
/**
 * Interface for handling events of type T
 *
 * @param <T> parameterized event of type T
 */
public interface EventHandler<T extends Event> {
	  void handle(T event);
}
