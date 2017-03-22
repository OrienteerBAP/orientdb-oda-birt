## OrientDB Birt Plugin (OrientDB ODA BIRT driver)

### What is this?

This is plugin for use [OrientDB](http://orientdb.com/) in [BIRT](https://www.eclipse.org/birt/) reports as DataSource. 
Can be used as [Eclipse plugin](https://orienteerbap.github.io/orientdb-oda-birt/) or [Maven package](insert maven archetype link here)- it is need, if you use [BIRT runtime package](https://mvnrepository.com/artifact/org.eclipse.birt.runtime). 

### How to...

#### How to install as Eclipse plugin

- Run you Eclipse
- Follow Help->Install new software
- Add resource site [https://orienteerbap.github.io/orientdb-oda-birt/](https://orienteerbap.github.io/orientdb-oda-birt/)
- Install plugin and all dependencies. Plugin versions equals OrientDB versions.

#### How to install as Maven package

Include in your POM next snippets:

Maven package dependency

```
<dependency>
  <groupId>org.orienteer</groupId>
  <artifactId>org.orienteer.birt.orientdb</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

Workaround for use OSGI module loading

```
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-dependency-plugin</artifactId>
    <executions>
        <execution>
            <id>copy</id>
            <phase>install</phase>
            <goals>
                <goal>copy-dependencies</goal>
            </goals>
            <configuration>
            	<includeArtifactIds>org.orienteer.birt.orientdb</includeArtifactIds>
                <outputDirectory>
                  ${project.build.outputDirectory}/WEB-INF/lib
                </outputDirectory>
            </configuration>
        </execution>
    </executions>
</plugin>	
```