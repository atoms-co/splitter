load("//libs/bazel/go:rules_go.bzl", "go_library", "go_test")

go_library(
    name = "p2quantile",
    srcs = glob(
        ["*.go"],
        exclude = ["*_test.go"],
    ),
    importpath = "atoms.co/infra/platform/wds2/pkg/util/p2quantile",
    visibility = ["//infra/platform/wds2:internal"],
)

go_test(
    name = "p2quantile_test",
    srcs = glob(["*_test.go"]),
    embed = [":p2quantile"],
    deps = [
        "@com_github_stretchr_testify//assert:go_default_library",
        "@com_github_stretchr_testify//require:go_default_library",
    ],
)
