package resourceManager.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of {@link Resource} comparison and manipulation interfaces.
 */
public abstract class ResourceCalculator {
	private static final Logger LOG = LoggerFactory.getLogger(ResourceCalculator.class);

	public abstract int 
	compare(Resource clusterResource, Resource lhs, Resource rhs);

	public static int divideAndCeil(int a, int b) {
		if (b == 0) {
			LOG.info("divideAndCeil called with a=" + a + " b=" + b);
			return 0;
		}
		return (a + (b - 1)) / b;
	}

	public static int roundUp(int a, int b) {
		return divideAndCeil(a, b) * b;
	}

	public static int roundDown(int a, int b) {
		return (a / b) * b;
	}

	/**
	 * Compute the number of containers which can be allocated given
	 * <code>available</code> and <code>required</code> resources.
	 * 
	 * @param available available resources
	 * @param required required resources
	 * @return number of containers which can be allocated
	 */
	public abstract int computeAvailableContainers(
			Resource available, Resource required);

	/**
	 * Multiply resource <code>r</code> by factor <code>by</code> 
	 * and normalize up using step-factor <code>stepFactor</code>.
	 * 
	 * @param r resource to be multiplied
	 * @param by multiplier
	 * @param stepFactor factor by which to normalize up 
	 * @return resulting normalized resource
	 */
	public abstract Resource multiplyAndNormalizeUp(
			Resource r, double by, Resource stepFactor);

	/**
	 * Multiply resource <code>r</code> by factor <code>by</code> 
	 * and normalize down using step-factor <code>stepFactor</code>.
	 * 
	 * @param r resource to be multiplied
	 * @param by multiplier
	 * @param stepFactor factor by which to normalize down 
	 * @return resulting normalized resource
	 */
	public abstract Resource multiplyAndNormalizeDown(
			Resource r, double by, Resource stepFactor);

	/**
	 * Normalize resource <code>r</code> given the base 
	 * <code>minimumResource</code> and verify against max allowed
	 * <code>maximumResource</code>
	 * 
	 * @param r resource
	 * @param minimumResource step-factor
	 * @param maximumResource the upper bound of the resource to be allocated
	 * @return normalized resource
	 */
	public Resource normalize(Resource r, Resource minimumResource,
			Resource maximumResource) {
		return normalize(r, minimumResource, maximumResource, minimumResource);
	}

	/**
	 * Normalize resource <code>r</code> given the base 
	 * <code>minimumResource</code> and verify against max allowed
	 * <code>maximumResource</code> using a step factor for hte normalization.
	 *
	 * @param r resource
	 * @param minimumResource minimum value
	 * @param maximumResource the upper bound of the resource to be allocated
	 * @param stepFactor the increment for resources to be allocated
	 * @return normalized resource
	 */
	public abstract Resource normalize(Resource r, Resource minimumResource,
			Resource maximumResource, 
			Resource stepFactor);


	/**
	 * Round-up resource <code>r</code> given factor <code>stepFactor</code>.
	 * 
	 * @param r resource
	 * @param stepFactor step-factor
	 * @return rounded resource
	 */
	public abstract Resource roundUp(Resource r, Resource stepFactor);

	/**
	 * Round-down resource <code>r</code> given factor <code>stepFactor</code>.
	 * 
	 * @param r resource
	 * @param stepFactor step-factor
	 * @return rounded resource
	 */
	public abstract Resource roundDown(Resource r, Resource stepFactor);

	/**
	 * Divide resource <code>numerator</code> by resource <code>denominator</code>
	 * using specified policy (domination, average, fairness etc.); hence overall
	 * <code>clusterResource</code> is provided for context.
	 *  
	 * @param clusterResource cluster resources
	 * @param numerator numerator
	 * @param denominator denominator
	 * @return <code>numerator</code>/<code>denominator</code> 
	 *         using specific policy
	 */
	public abstract float divide(
			Resource clusterResource, Resource numerator, Resource denominator);

	/**
	 * Ratio of resource <code>a</code> to resource <code>b</code>.
	 * 
	 * @param a resource 
	 * @param b resource
	 * @return ratio of resource <code>a</code> to resource <code>b</code>
	 */
	public abstract float ratio(Resource a, Resource b);

	/**
	 * Divide-and-ceil <code>numerator</code> by <code>denominator</code>.
	 * 
	 * @param numerator numerator resource
	 * @param denominator denominator
	 * @return resultant resource
	 */
	public abstract Resource divideAndCeil(Resource numerator, int denominator);

}
