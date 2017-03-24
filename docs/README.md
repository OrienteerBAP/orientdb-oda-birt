## OrientDB Birt Plugin (OrientDB ODA BIRT driver)

### What is this?

This is plugin for use [OrientDB](http://orientdb.com/) in [BIRT](https://www.eclipse.org/birt/) reports as DataSource. 
Can be used as [Eclipse plugin](https://orienteerbap.github.io/orientdb-oda-birt/) or Maven package - it is need, if you use [BIRT runtime package](https://mvnrepository.com/artifact/org.eclipse.birt.runtime). 

### How to...

#### How to install as Eclipse plugin

- Run you Eclipse
- Follow Help->Install new software
- Add resource site [https://orienteerbap.github.io/orientdb-oda-birt/](https://orienteerbap.github.io/orientdb-oda-birt/)
- Install plugin and all dependencies. Plugin versions equals OrientDB versions.

You may use it with binary or HTTP OrientDB API. Now not support HTTP API parameters.

#### How to install as Maven package

Include in your POM next snippets:

Sonatype snapshot repository

```
<repositories>
	<repository>
		<id>snapshots-repo</id>
		<url>https://oss.sonatype.org/content/repositories/snapshots</url>
		<releases>
			<enabled>false</enabled>
		</releases>
		<snapshots>
			<enabled>true</enabled>
		</snapshots>
	</repository>
</repositories>
```

Maven package dependency

```
<dependency>
  <groupId>org.orienteer</groupId>
  <artifactId>org.orienteer.birt.orientdb</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

