using Documenter
using DocumenterVitepress
using Natter
using TOML

const SOURCE_REPO = get(ENV, "DOCUMENTER_SOURCE_REPO", "https://github.com/rbeeli/Natter.jl")
const FORMAT_REPO = get(ENV, "DOCUMENTER_FORMAT_REPO", "github.com/rbeeli/Natter.jl")
const DEPLOY_REPO = get(ENV, "DOCUMENTER_REPO", "github.com/rbeeli/Natter.jl.git")
const BUILD_VITEPRESS = parse(Bool, get(ENV, "DOCUMENTER_BUILD_VITEPRESS", "false"))
const DEPLOY_DOCS = parse(Bool, get(ENV, "DOCUMENTER_DEPLOY", "false"))
const DEPLOY_URL_ENV = get(ENV, "DOCUMENTER_DEPLOY_URL", "")
const DEPLOY_URL = isempty(DEPLOY_URL_ENV) ? nothing : DEPLOY_URL_ENV
const PACKAGE_VERSION = TOML.parsefile(joinpath(@__DIR__, "..", "Project.toml"))["version"]

function deploy_decision()
    decision = Documenter.deploy_folder(
        Documenter.auto_detect_deploy_system();
        repo=FORMAT_REPO,
        devbranch="main",
        devurl="dev",
        push_preview=true,
    )

    if decision.all_ok && !decision.is_preview && decision.subfolder == "dev"
        return Documenter.DeployDecision(;
            all_ok=decision.all_ok,
            branch=decision.branch,
            is_preview=decision.is_preview,
            repo=decision.repo,
            subfolder="",
        )
    end

    return decision
end

makedocs(;
    modules=[Natter],
    authors="Natter.jl contributors",
    repo=SOURCE_REPO,
    sitename="Natter.jl",
    format=DocumenterVitepress.MarkdownVitepress(;
        repo=FORMAT_REPO,
        devbranch="main",
        devurl="dev",
        deploy_url=DEPLOY_URL,
        description="Documentation for Natter.jl, a Julia client for NATS.",
        build_vitepress=BUILD_VITEPRESS,
        deploy_decision=deploy_decision(),
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
    Documenter.deploydocs(;
        root=@__DIR__,
        repo=DEPLOY_REPO,
        target=joinpath("build", "1"),
        versions=nothing,
        devbranch="main",
        push_preview=true,
    )
end
