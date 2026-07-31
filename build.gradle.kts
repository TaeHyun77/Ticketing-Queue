plugins {
	kotlin("jvm") version "1.9.25"
	kotlin("plugin.spring") version "1.9.25"
	id("org.springframework.boot") version "3.5.4"
	id("io.spring.dependency-management") version "1.1.7"
}

group = "com.example"
version = "0.0.1-SNAPSHOT"

java {
	toolchain {
		languageVersion = JavaLanguageVersion.of(17)
	}
}

repositories {
	mavenCentral()
}

dependencies {

	// webflux
	implementation("org.springframework.boot:spring-boot-starter-webflux")

	// kotlin
	implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
	implementation("io.projectreactor.kotlin:reactor-kotlin-extensions")
	implementation("org.jetbrains.kotlin:kotlin-reflect")
	implementation("org.jetbrains.kotlinx:kotlinx-coroutines-reactor")

	testImplementation("org.springframework.boot:spring-boot-starter-test")
	testImplementation("io.projectreactor:reactor-test")
	testImplementation("org.jetbrains.kotlin:kotlin-test-junit5")
	testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test")
	testRuntimeOnly("org.junit.platform:junit-platform-launcher")

	// security 의존성
	implementation("org.springframework.boot:spring-boot-starter-security")


	// log 의존성
	implementation("io.github.oshai:kotlin-logging-jvm:5.1.4")

	// redis 의존성
	implementation("org.springframework.boot:spring-boot-starter-data-redis-reactive")

	// 모니터링 설정
	implementation ("org.springframework.boot:spring-boot-starter-actuator")
	implementation ("io.micrometer:micrometer-registry-prometheus")
}

kotlin {
	compilerOptions {
		freeCompilerArgs.addAll("-Xjsr305=strict")
	}
}

tasks.withType<Test> {
	useJUnitPlatform()
}
