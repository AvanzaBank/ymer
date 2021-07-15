package com.avanza.ymer.util;

public final class GigaSpacesPartitionIdUtil {
	private GigaSpacesPartitionIdUtil() {
	}

	public static int getPartitionId(Object routingKey, int partitionCount) {
		return safeAbsoluteValue(routingKey.hashCode()) % partitionCount + 1;
	}

	private static int safeAbsoluteValue(int value) {
		return value == Integer.MIN_VALUE ? Integer.MAX_VALUE : Math.abs(value);
	}

}
