
package recoverable;

import recoverable.stateStore.RMStateStore.RMState;

/**
 * 备忘录模式
 * @author 无言的雨
 *
 */
public interface Recoverable {
  public void recover(RMState state) throws Exception;
}