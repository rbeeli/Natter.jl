using Documenter
using DocumenterVitepress
using Natter
using TOML

const FORMAT_REPO = get(ENV, "DOCUMENTER_FORMAT_REPO", "https://github.com/rbeeli/Natter.jl")
const DEPLOY_REPO = get(ENV, "DOCUMENTER_REPO", "github.com/rbeeli/Natter.jl.git")
const BUILD_VITEPRESS = parse(Bool, get(ENV, "DOCUMENTER_BUILD_VITEPRESS", "false"))
const DEPLOY_DOCS = parse(Bool, get(ENV, "DOCUMENTER_DEPLOY", "false"))
const DEPLOY_URL = get(ENV, "DOCUMENTER_DEPLOY_URL", nothing)
const PACKAGE_VERSION = TOML.parsefile(joinpath(@__DIR__, "..", "Project.toml"))["version"]

makedocs(;
    modules=[Natter],
    authors="Natter.jl contributors",
    repo=FORMAT_REPO,
    sitename="Natter.jl",
    format=DocumenterVitepress.MarkdownVitepress(;
        repo=FORMAT_REPO,
        devbranch="main",
        devurl="dev",
        deploy_url=DEPLOY_URL,
        description="Documentation for Natter.jl, a Julia client for NATS.",
        build_vitepress=BUILD_VITEPRESS,
        inventory_version=PACKAGE_VERSION,
        sidebar_drawer=true,
    ),
    source="src",
    build="build",
    checkdocs=:none,
    pages=[
        "Home" => "index.md",
        "Getting Started" => "getting-started.md",
        "Guides" => [
            "Core Messaging" => "core.md",
            "JetStream" => "jetstream.md",
            "KeyValue" => "keyvalue.md",
            "Reliability And TLS" => "reliability.md",
        ],
        "Examples" => [
            "Basic Publish And Subscribe" => "examples/basic-pub-sub.md",
            "Request Reply Service" => "examples/request-reply.md",
            "JetStream Work Queue" => "examples/jetstream-work-queue.md",
            "KeyValue Store" => "examples/keyvalue-store.md",
            "Production Client" => "examples/production-client.md",
        ],
        "Reference" => "reference.md",
        "Feature Coverage" => "feature-coverage.md",
    ],
)

if DEPLOY_DOCS
    DocumenterVitepress.deploydocs(;
        repo=DEPLOY_REPO,
        target=joinpath(@__DIR__, "build"),
        branch="gh-pages",
        devbranch="main",
        push_preview=true,
    )
end
