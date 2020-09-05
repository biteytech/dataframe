package tech.bitey.dataframe;

import java.util.function.Function;

/**
 * Represents a function that produces a byte-valued result. This is the
 * {@code byte}-producing primitive specialization for {@link Function}.
 *
 * @param <T> the type of the input to the function
 *
 * @see Function
 */
@FunctionalInterface
public interface ToByteFunction<T> {

	/**
	 * Applies this function to the given argument.
	 *
	 * @param value the function argument
	 * @return the function result
	 */
	byte applyAsByte(T value);
}
