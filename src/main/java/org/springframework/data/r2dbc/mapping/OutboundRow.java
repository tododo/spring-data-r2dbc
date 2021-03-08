/*
 * Copyright 2019-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.mapping;

import io.r2dbc.spi.Row;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import org.springframework.util.Assert;

/**
 * Representation of a {@link Row} to be written through a {@code INSERT} or {@code UPDATE} statement.
 *
 * @author Mark Paluch
 * @see SettableValue
 */
public class OutboundRow implements Map<String, SettableValue> {

	private final Map<String, SettableValue> rowAsMap;

	/**
	 * Creates an empty {@link OutboundRow} instance.
	 */
	public OutboundRow() {
		this.rowAsMap = new LinkedHashMap<>();
	}

	/**
	 * Creates a new {@link OutboundRow} from a {@link Map}.
	 *
	 * @param map the map used to initialize the {@link OutboundRow}.
	 */
	public OutboundRow(Map<String, SettableValue> map) {

		Assert.notNull(map, "Map must not be null");

		this.rowAsMap = new LinkedHashMap<>(map);
	}

	/**
	 * Create a {@link OutboundRow} instance initialized with the given key/value pair.
	 *
	 * @param key key.
	 * @param value value.
	 */
	public OutboundRow(String key, SettableValue value) {
		this.rowAsMap = new LinkedHashMap<>();
		this.rowAsMap.put(key, value);
	}

	/**
	 * Put the given key/value pair into this {@link OutboundRow} and return this. Useful for chaining puts in a single
	 * expression:
	 *
	 * <pre class="code">
	 * row.append("a", 1).append("b", 2)}
	 * </pre>
	 *
	 * @param key key.
	 * @param value value.
	 * @return this
	 */
	public OutboundRow append(String key, SettableValue value) {
		this.rowAsMap.put(key, value);
		return this;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#size()
	 */
	@Override
	public int size() {
		return this.rowAsMap.size();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return this.rowAsMap.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsKey(java.lang.Object)
	 */
	@Override
	public boolean containsKey(Object key) {
		return this.rowAsMap.containsKey(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value) {
		return this.rowAsMap.containsValue(value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#get(java.lang.Object)
	 */
	@Override
	public SettableValue get(Object key) {
		return this.rowAsMap.get(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#put(java.lang.Object, java.lang.Object)
	 */
	@Override
	public SettableValue put(String key, SettableValue value) {
		return this.rowAsMap.put(key, value);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#remove(java.lang.Object)
	 */
	@Override
	public SettableValue remove(Object key) {
		return this.rowAsMap.remove(key);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#putAll(java.util.Map)
	 */
	@Override
	public void putAll(Map<? extends String, ? extends SettableValue> m) {
		this.rowAsMap.putAll(m);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#clear()
	 */
	@Override
	public void clear() {
		this.rowAsMap.clear();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#keySet()
	 */
	@Override
	public Set<String> keySet() {
		return this.rowAsMap.keySet();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#values()
	 */
	@Override
	public Collection<SettableValue> values() {
		return this.rowAsMap.values();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.Map#entrySet()
	 */
	@Override
	public Set<Entry<String, SettableValue>> entrySet() {
		return this.rowAsMap.entrySet();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(final Object o) {

		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		OutboundRow row = (OutboundRow) o;

		return this.rowAsMap.equals(row.rowAsMap);
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return this.rowAsMap.hashCode();
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "OutboundRow[" + this.rowAsMap + "]";
	}

	@Override
	public void forEach(BiConsumer<? super String, ? super SettableValue> action) {
		this.rowAsMap.forEach(action);
	}
}
