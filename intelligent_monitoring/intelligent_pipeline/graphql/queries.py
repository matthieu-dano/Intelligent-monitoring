LIST_JOB = """
    query JobsQuery(
        $repositoryLocationName: String!
        $repositoryName: String!
    ) {
        repositoryOrError(
            repositorySelector: {
                repositoryLocationName: $repositoryLocationName
                repositoryName: $repositoryName
            }
        ) {
            ... on Repository {
                jobs {
                    name
                }
            }
        }
    }
"""

LAUNCH_JOB = """
mutation LaunchRunMutation(
          $repositoryLocationName: String!
          $repositoryName: String!
          $jobName: String!
          $runConfigData: RunConfigData!
        ) {
          launchRun(
            executionParams: {
              selector: {
                repositoryLocationName: $repositoryLocationName
                repositoryName: $repositoryName
                jobName: $jobName
              }
              runConfigData: $runConfigData
            }
          ) {
            __typename
            ... on LaunchRunSuccess {
              run {
                runId
              }
            }
            ... on RunConfigValidationInvalid {
              errors {
                message
                reason
              }
            }
            ... on PythonError {
              message
            }
          }
        }
"""

START_SCHEDULE = """
mutation StartScheduleMutation(
  $repositoryLocationName: String!
  $repositoryName: String!
  $scheduleName: String!
) {
  startSchedule(
    scheduleSelector: {
      repositoryLocationName: $repositoryLocationName
      repositoryName: $repositoryName
      scheduleName: $scheduleName
    }
  ) {
    __typename
    ... on StartScheduleSuccess {
      scheduleState {
        id
        status
      }
    }
    ... on PythonError {
      message
    }
  }
}
"""

CANCEL_SCHEDULE = """
mutation StopScheduleMutation(
  $repositoryLocationName: String!
  $repositoryName: String!
  $scheduleName: String!
) {
  stopRunningSchedule(
    scheduleSelector: {
      repositoryLocationName: $repositoryLocationName
      repositoryName: $repositoryName
      scheduleName: $scheduleName
    }
  ) {
    __typename
    ... on StopScheduleSuccess {
      scheduleState {
        id
        status
      }
    }
    ... on PythonError {
      message
    }
  }
}
"""

UPDATE_SCHEDULE = """
mutation UpdateScheduleMutation(
  $repositoryLocationName: String!
  $repositoryName: String!
  $scheduleName: String!
  $cronSchedule: String!
) {
  updateSchedule(
    scheduleSelector: {
      repositoryLocationName: $repositoryLocationName
      repositoryName: $repositoryName
      scheduleName: $scheduleName
    }
    cronSchedule: $cronSchedule
  ) {
    __typename
    ... on Schedule {
      id
      cronSchedule
    }
    ... on PythonError {
      message
    }
  }
}
"""
