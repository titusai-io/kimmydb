// The day-to-day CI gate for kimmydb. Jenkins runs what a change has to pass
// before it merges; the tag-driven release stays on GitHub Actions and is not
// duplicated here (ADR-113).
//
// The split is deliberate and the release half cannot move: the macOS binaries
// have no agent to build on, the arm64 artifacts have no arm64 agent and
// ADR-063 already rejected QEMU as an hour per architecture, and the ADR-108
// build provenance is signed against GitHub's OIDC workload identity, which
// only GitHub can issue. That last one is dormant while the repository is
// private and arms itself the day it goes public, which is exactly how it
// would get thrown away by mistake.
//
// Configure this as a MULTIBRANCH PIPELINE, not a single-branch job. Three
// consequences, all learned on a sibling project rather than here:
//
//   1. lan.example.com is a LAN-private zone, so github.com cannot deliver a
//      webhook to this controller. Branch and PR discovery is a periodic SCAN
//      (2 minutes). A push is picked up on the next scan, not instantly.
//   2. A Multibranch job needs a GitHub App or PAT credential -- a deploy key
//      cannot call the API to enumerate branches and pull requests, and this
//      repository is private.
//   3. Add "Filter by name (with regular expression)" with ^(main|PR-\d+)$.
//      Without it, a feature branch pushed before its PR is opened gets a
//      branch job on the next scan; when the PR then appears, "Exclude
//      branches that are also filed as PRs" orphans and disables that job --
//      after it has told GitHub a build is running and before it can report a
//      result. The commit is then stuck pending forever with nothing running.
//
// This pipeline is written to be REVERSIBLE. Every stage is a thin wrapper
// around the same command the repository already documents as a gate, so
// Actions and Jenkins run identical checks and neither becomes the only place
// something happens. Getting off Jenkins is then: re-enable ci.yml's triggers,
// swap the one required status check back, stop this job.

pipeline {
  // Pinned to an agent that has Docker, which the controller does not. The
  // label resolves to the build host: x86_64, 8 cores, 31 GiB, 4 executors.
  agent { label 'docker' }

  options {
    // Only the newest result on a branch is ever read. This is the same saving
    // as ci.yml's `cancel-in-progress`, and note that ci.yml deliberately does
    // NOT cancel on main -- a cancelled run there is a commit whose verdict was
    // never delivered. Jenkins builds main from its own scan rather than from a
    // push storm, so the race that motivated the exception does not arise.
    disableConcurrentBuilds(abortPrevious: true)
    buildDiscarder(logRotator(numToKeepStr: '20'))
    timestamps()
    // The workspace test suite is the long pole; 60 minutes is generous enough
    // that a timeout means something is wrong rather than merely slow.
    timeout(time: 60, unit: 'MINUTES')
  }

  environment {
    // The image the Dockerfile's own build stage uses, so CI compiles against
    // the toolchain the shipped binary is compiled with.
    RUST_IMAGE = 'rust:1-slim-trixie'

    // A named volume shared by every branch and every build, holding the
    // crates.io registry and git checkouts. This is the half of the cache that
    // is identical for everyone; the other half -- target/ -- lives in the
    // per-branch workspace, which is what makes an incremental build possible
    // at all. A cold GitHub runner cannot have either, and that difference is
    // the entire reason this is faster, not the CPU.
    CARGO_VOLUME = 'kimmydb-cargo-registry'

    // **Separate target directories per stage, and this is not tidiness.**
    // clippy writes check metadata and `cargo test` writes test binaries; they
    // are different rustc invocations over the same crates, so sharing one
    // target/ makes each one invalidate the other's artifacts on every build.
    // ci.yml says so in as many words -- "a cache saved by the other would be
    // a cold start with extra steps" -- and gives each job its own cache for
    // exactly this reason.
    //
    // Measured here before it was believed: sharing one directory, the second
    // build was SLOWER than the first (976s against 887s), with lint dropping
    // 75s to 33s while the test stage rose 751s to 889s. The cache was working;
    // the two stages were thrashing it.
    //
    // The cluster harness deliberately shares the test directory: it is also a
    // `cargo test` build of the same workspace, so it wants those artifacts.
    LINT_TARGET_DIR = '/src/target/ci-lint'
    TEST_TARGET_DIR = '/src/target/ci-test'

    // Matches ci.yml so a failure here means the same thing there.
    CARGO_TERM_COLOR = 'always'
    // Debug info roughly doubles build time and nothing here reads a symbol
    // table.
    CARGO_PROFILE_TEST_DEBUG = '0'
    RUSTFLAGS = '-D warnings'
  }

  stages {
    stage('Lint') {
      steps {
        // `docker run` with plain sh rather than `agent { docker { ... } }`:
        // this controller has no Docker Pipeline plugin, and the declarative
        // docker agent fails at parse time without it. Every job here is
        // written this way, so it is house style rather than a workaround.
        //
        // Runs as root, then hands the workspace back at the end. Cargo writes
        // into target/ as whoever the container is, and a root-owned target/ is
        // a workspace Jenkins cannot clean up. The chown is inside the same
        // shell so it happens even when a check fails -- hence `|| rc=$?`
        // rather than `set -e` around the checks themselves.
        //
        // The memory caps are STARTING values, not measured ones. The build host
        // runs the development cluster this database is tested against, and an
        // unbounded build is the one thing on that host that could disturb it.
        // Measure the real peak (`docker stats` while a build runs) and bring
        // these down to roughly 2.5x observed, the way a sibling project's were
        // set.
        sh '''
          set -e
          docker volume create "${CARGO_VOLUME}" >/dev/null
          docker run --rm \
            --memory=6g --memory-swap=6g \
            -v "${WORKSPACE}":/src -w /src \
            -v "${CARGO_VOLUME}":/cargo \
            -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR="${LINT_TARGET_DIR}" \
            -e CARGO_TERM_COLOR -e CARGO_PROFILE_TEST_DEBUG -e RUSTFLAGS \
            -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
            "${RUST_IMAGE}" sh -c '
              set -e
              rustup component add rustfmt clippy >/dev/null
              rc=0
              cargo fmt --all --check || rc=$?
              cargo clippy --workspace --all-targets -- -D warnings || rc=$?
              chown -R "${HOST_UID}:${HOST_GID}" /src
              exit $rc
            '
        '''
      }
    }

    stage('Test') {
      steps {
        // `cargo test --workspace`, which is the gate the repository documents,
        // rather than ci.yml's `cargo nextest run` plus a separate doctest run.
        // nextest is a CI speed optimisation that would have to be installed
        // into the container on every build; one command that covers unit,
        // integration and doc tests keeps this a thin wrapper over the
        // documented gate, which is what makes the two systems comparable.
        //
        // A C toolchain is needed: ring compiles C and assembly, and the slim
        // image ships no compiler.
        sh '''
          set -e
          docker run --rm \
            --memory=10g --memory-swap=10g \
            -v "${WORKSPACE}":/src -w /src \
            -v "${CARGO_VOLUME}":/cargo \
            -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR="${TEST_TARGET_DIR}" \
            -e CARGO_TERM_COLOR -e CARGO_PROFILE_TEST_DEBUG -e RUSTFLAGS \
            -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
            "${RUST_IMAGE}" sh -c '
              set -e
              apt-get update -qq && apt-get install -y -qq --no-install-recommends \
                gcc libc6-dev pkg-config >/dev/null
              rc=0
              cargo test --workspace || rc=$?
              chown -R "${HOST_UID}:${HOST_GID}" /src
              exit $rc
            '
        '''
      }
    }

    stage('Native deps') {
      steps {
        // The dependency-policy gate. It is cheap and it is the one check that
        // catches a supply-chain surprise arriving through a version bump.
        //
        // Runs on Linux, which matters: on macOS it fails for a pre-existing,
        // platform-specific reason (core-foundation-sys and
        // system-configuration-sys arrive through hickory-resolver). Anyone
        // debugging a green Jenkins and a red laptop should read that as the
        // platform difference it is, not as a Jenkins fault.
        sh '''
          set -e
          docker run --rm \
            --memory=4g --memory-swap=4g \
            -v "${WORKSPACE}":/src -w /src \
            -v "${CARGO_VOLUME}":/cargo \
            -e CARGO_HOME=/cargo \
            -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
            "${RUST_IMAGE}" sh -c '
              set -e
              rc=0
              ./scripts/check-native-deps.sh || rc=$?
              chown -R "${HOST_UID}:${HOST_GID}" /src
              exit $rc
            '
        '''
      }
    }

    stage('Cluster harness') {
      // main only, as it is on Actions: it spawns real nodes and is the
      // slowest check that is not the workspace suite.
      when { branch 'main' }
      steps {
        // Single-threaded by the test's own requirement, and the nodes bind
        // 127.0.0.1:0 -- ephemeral ports -- so this cannot collide with the
        // cluster the build host is hosting. Verified before this was written;
        // re-check if a new integration test ever binds a fixed port.
        sh '''
          set -e
          docker run --rm \
            --memory=8g --memory-swap=8g \
            -v "${WORKSPACE}":/src -w /src \
            -v "${CARGO_VOLUME}":/cargo \
            -e CARGO_HOME=/cargo -e CARGO_TARGET_DIR="${TEST_TARGET_DIR}" \
            -e CARGO_TERM_COLOR -e CARGO_PROFILE_TEST_DEBUG -e RUSTFLAGS \
            -e HOST_UID="$(id -u)" -e HOST_GID="$(id -g)" \
            "${RUST_IMAGE}" sh -c '
              set -e
              apt-get update -qq && apt-get install -y -qq --no-install-recommends \
                gcc libc6-dev pkg-config >/dev/null
              rc=0
              cargo test -p kimmyd --test cluster -- --ignored --test-threads=1 || rc=$?
              chown -R "${HOST_UID}:${HOST_GID}" /src
              exit $rc
            '
        '''
      }
    }
  }

  post {
    always {
      // Disk is the failure mode most likely to bite on a persistent agent: a
      // Rust target/ directory runs to tens of gigabytes per branch, and the
      // build host filling up is everyone's problem rather than CI's. Reported
      // every build so the trend is visible before it is urgent.
      sh '''
        echo "--- workspace target/ ---"
        du -sh "${WORKSPACE}/target" 2>/dev/null || echo "no target/ yet"
        echo "--- shared cargo volume ---"
        docker system df -v 2>/dev/null | grep -F "${CARGO_VOLUME}" || true
        echo "--- agent disk ---"
        df -h "${WORKSPACE}" | tail -1
      '''
    }
  }
}
