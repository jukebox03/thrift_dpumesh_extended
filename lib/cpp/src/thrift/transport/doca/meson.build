project('dpumesh_dpu', 'c',
  version : '1.0.0',
  default_options : ['buildtype=debug'],
  meson_version : '>=0.61.2'
)

program_name = 'dpa_kernel'

add_project_arguments('-DDOCA_ALLOW_EXPERIMENTAL_API', language: 'c')

inc_dirs = []
inc_dirs += include_directories('/opt/mellanox/doca/include/')
inc_dirs += include_directories('..')   # for dpumesh.h

deps = []
deps += dependency('doca-common')
deps += dependency('doca-comch')
deps += dependency('doca-argp')
deps += dependency('doca-dpa')
deps += dependency('libflexio')

srcs = [
    'dpu_main.c',
    'dpu_worker.c',
    'config.c',
    'object.c',
    'common.c',
    'buffer.c',
    'ring.c',
    'comch_common.c',
    'comch_server.c',
    'comch_client.c',
    'comch_consumer.c',
    'comch_producer.c',
    'comch_msgq.c',
    'dpa.c',
    'dma.c',
]

# Build DPA kernel via DPACC
dpacc_build_script = meson.project_source_root() / 'build_dpacc.sh'
device_src = meson.project_source_root() / 'device' / 'dpa_kernel.c'
dpacc_mcpu_flag = 'nv-dpa-bf3'
doca_dep = dependency('doca-dpa')
doca_lib_dir = doca_dep.get_variable(pkgconfig : 'libdir')

result = run_command(dpacc_build_script,
    meson.project_build_root(),
    meson.project_source_root(),
    device_src,
    program_name,
    dpacc_mcpu_flag,
    doca_lib_dir,
    check: true)

device_build_dir = meson.project_build_root() / 'device'
dpa_program_lib = meson.get_compiler('c').find_library(
    program_name,
    dirs: device_build_dir,
    static: true
)
deps += dpa_program_lib

executable('dpumesh_dpu', srcs,
  dependencies : deps,
  include_directories : inc_dirs,
  install : false,
)
