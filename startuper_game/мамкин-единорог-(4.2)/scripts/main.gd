extends Control

@onready var metrics_label: Label = $Margin/VBox/Metrics
@onready var tasks_label: Label = $Margin/VBox/Tasks
@onready var event_label: Label = $Margin/VBox/Event
@onready var log_label: Label = $Margin/VBox/Log
@onready var choose_a: Button = $Margin/VBox/EventButtons/ChooseA
@onready var choose_b: Button = $Margin/VBox/EventButtons/ChooseB
@onready var next_week_button: Button = $Margin/VBox/NextWeek

var state := GameState.new()
var weekly_tasks: Array[Dictionary] = []
var current_event: Dictionary = {}
var selected_tasks: Array[Dictionary] = []
var waiting_event_choice := false

func _ready() -> void:
	prepare_week()
	next_week_button.pressed.connect(_on_next_week_pressed)
	choose_a.pressed.connect(func() -> void: _resolve_event(0))
	choose_b.pressed.connect(func() -> void: _resolve_event(1))

func prepare_week() -> void:
	if state.game_over:
		_show_game_over()
		return

	weekly_tasks = state.get_weekly_tasks()
	selected_tasks.clear()
	_render_tasks()
	current_event = state.pick_event()
	event_label.text = "Событие недели: %s\nA) %s\nB) %s" % [
		current_event.title,
		current_event.options[0].text,
		current_event.options[1].text
	]
	log_label.text = "План недели: выбери до %d задач и нажми 'Завершить неделю'." % GameState.TASKS_PER_WEEK
	metrics_label.text = state.get_metrics_text()
	waiting_event_choice = false
	choose_a.disabled = true
	choose_b.disabled = true
	next_week_button.disabled = false

func _render_tasks() -> void:
	var rows: Array[String] = []
	for i in weekly_tasks.size():
		var task_name := weekly_tasks[i].name
		rows.append("%d) %s" % [i + 1, task_name])
	tasks_label.text = "Бэклог недели:\n%s\n\nНажми клавиши 1-5 чтобы выбрать задачи." % "\n".join(rows)

func _unhandled_key_input(event: InputEvent) -> void:
	if event is InputEventKey and event.pressed and not event.echo:
		var key := event.keycode
		if key >= KEY_1 and key <= KEY_5:
			var idx := int(key - KEY_1)
			_toggle_task(idx)

func _toggle_task(idx: int) -> void:
	if idx < 0 or idx >= weekly_tasks.size():
		return
	var item := weekly_tasks[idx]
	if selected_tasks.has(item):
		selected_tasks.erase(item)
	elif selected_tasks.size() < GameState.TASKS_PER_WEEK:
		selected_tasks.append(item)
	_update_selected_text()

func _update_selected_text() -> void:
	var selected_names: Array[String] = []
	for t in selected_tasks:
		selected_names.append(t.name)
	if selected_names.is_empty():
		log_label.text = "Пока ничего не выбрано. Нужно до %d задач." % GameState.TASKS_PER_WEEK
	else:
		log_label.text = "Выбрано (%d/%d): %s" % [
			selected_tasks.size(),
			GameState.TASKS_PER_WEEK,
			", ".join(selected_names)
		]

func _on_next_week_pressed() -> void:
	if selected_tasks.is_empty():
		log_label.text = "Выбери хотя бы одну задачу перед завершением недели."
		return
	var logs := state.apply_tasks(selected_tasks)
	log_label.text = "%s\n\nВыбери реакцию на событие (A/B)." % "\n".join(logs)
	waiting_event_choice = true
	choose_a.disabled = false
	choose_b.disabled = false
	next_week_button.disabled = true

func _resolve_event(option_idx: int) -> void:
	if not waiting_event_choice:
		return
	var event_log := state.apply_event_option(current_event, option_idx)
	state.finish_week()
	metrics_label.text = state.get_metrics_text()
	log_label.text = "%s\n\nНеделя закрыта." % event_log
	choose_a.disabled = true
	choose_b.disabled = true
	waiting_event_choice = false
	prepare_week()

func _show_game_over() -> void:
	tasks_label.text = "Партия завершена."
	event_label.text = "Итог: %s" % state.game_over_reason
	log_label.text = "Нажми Cmd/Ctrl+R для перезапуска сцены."
	choose_a.disabled = true
	choose_b.disabled = true
	next_week_button.disabled = true
	metrics_label.text = state.get_metrics_text()
