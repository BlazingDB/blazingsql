package com.blazingdb.calcite.application;

public final class PeopleSchema {
	public final Person[] HEROES = {new Person("Ironman", 12), new Person("Batman", 10)};

	public static class Person {
		public final String NAME;
		public final int AGE;

		public Person(final String NAME, final int AGE) {
			this.NAME = NAME;
			this.AGE = AGE;
		}
	}
}
