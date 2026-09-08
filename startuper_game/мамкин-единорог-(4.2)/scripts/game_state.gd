extends RefCounted
class_name GameState

const TOTAL_WEEKS := 12
const TASKS_PER_WEEK := 3

var week: int = 1
var cash: float = 120000.0
var revenue: float = 15000.0
var burn_rate: float = 22000.0
var morale: float = 60.0
var tech_debt: float = 25.0
var activation: float = 30.0
var retention: float = 28.0
var reputation: float = 50.0

var game_over: bool = false
var game_over_reason: String = ""

var task_pool: Array[Dictionary] = []
var event_pool: Array[Dictionary] = []

func _init() -> void:
	task_pool = _default_tasks()
	event_pool = _default_events()

func get_runway_months() -> float:
	return cash / max(burn_rate, 1.0)

func get_metrics_text() -> String:
	return "Неделя %d/12 | Cash: %.0f | Runway: %.1f мес | MRR: %.0f\nMorale: %.0f | Debt: %.0f | Activation: %.0f | Retention: %.0f | Reputation: %.0f" % [
		week, cash, get_runway_months(), revenue, morale, tech_debt, activation, retention, reputation
	]

func get_weekly_tasks() -> Array[Dictionary]:
	task_pool.shuffle()
	return task_pool.slice(0, 5)

func pick_event() -> Dictionary:
	event_pool.shuffle()
	return event_pool[0]

func apply_tasks(selected_tasks: Array[Dictionary]) -> Array[String]:
	var logs: Array[String] = []
	for task in selected_tasks:
		for key in task.effects.keys():
			_apply_effect(key, float(task.effects[key]))
		logs.append("Задача: %s" % task.name)
	return logs

func apply_event_option(event_data: Dictionary, option_index: int) -> String:
	var options: Array = event_data.options
	var option: Dictionary = options[option_index]
	for key in option.effects.keys():
		_apply_effect(key, float(option.effects[key]))
	return "Событие: %s -> %s" % [event_data.title, option.text]

func finish_week() -> void:
	cash += revenue - burn_rate
	week += 1
	_check_end_conditions()

func _apply_effect(key: String, delta: float) -> void:
	match key:
		"cash":
			cash += delta
		"revenue":
			revenue += delta
		"burn_rate":
			burn_rate += delta
		"morale":
			morale = clamp(morale + delta, 0.0, 100.0)
		"tech_debt":
			tech_debt = clamp(tech_debt + delta, 0.0, 100.0)
		"activation":
			activation = clamp(activation + delta, 0.0, 100.0)
		"retention":
			retention = clamp(retention + delta, 0.0, 100.0)
		"reputation":
			reputation = clamp(reputation + delta, 0.0, 100.0)

func _check_end_conditions() -> void:
	if cash <= 0:
		game_over = true
		game_over_reason = "Деньги закончились. Мамкин ищет работу в корпорации."
		return
	if morale <= 5:
		game_over = true
		game_over_reason = "Команда выгорела. Даже ирония больше не спасает."
		return
	if week > TOTAL_WEEKS:
		game_over = true
		if retention >= 35.0 and activation >= 40.0 and cash > 0.0:
			game_over_reason = "Успех! Мамкин почти единорог: метрики и деньги в норме."
		elif cash > 0.0:
			game_over_reason = "Выжили, но PMF еще не пойман. Время на пивот."
		else:
			game_over_reason = "Квартал закрыт в минус. Инвестор читает нотации."

func _default_tasks() -> Array[Dictionary]:
	return [
		{
			"name": "Запустить фичу 'умные рекомендации'",
			"effects": {"activation": 5, "tech_debt": 4, "morale": -2}
		},
		{
			"name": "Починить 10 критических багов",
			"effects": {"retention": 4, "tech_debt": -5, "morale": -1, "reputation": 2}
		},
		{
			"name": "Провести CustDev (8 интервью)",
			"effects": {"activation": 2, "retention": 2, "reputation": 1}
		},
		{
			"name": "Агрессивный таргет в соцсетях",
			"effects": {"activation": 4, "burn_rate": 1500, "cash": -3000}
		},
		{
			"name": "Рефакторинг модуля оплаты",
			"effects": {"tech_debt": -6, "retention": 2, "morale": -2}
		},
		{
			"name": "Сделать демо для инвестора",
			"effects": {"cash": 12000, "morale": -3, "reputation": 1}
		},
		{
			"name": "Внедрить онбординг в 3 шага",
			"effects": {"activation": 3, "retention": 2}
		},
		{
			"name": "Нанять фриланс-разработчика на спринт",
			"effects": {"tech_debt": 2, "burn_rate": 2500, "morale": 1}
		}
	]

func _default_events() -> Array[Dictionary]:
	return [
		{
			"title": "Инвестор спрашивает: где x3 рост?",
			"options": [
				{
					"text": "Обещать и ускорить релиз любой ценой",
					"effects": {"activation": 3, "tech_debt": 5, "morale": -4}
				},
				{
					"text": "Показать честные метрики и план",
					"effects": {"reputation": 3, "cash": -2000}
				}
			]
		},
		{
			"title": "Прод упал вечером в пятницу.",
			"options": [
				{
					"text": "Экстренно чиним ночью",
					"effects": {"retention": 2, "morale": -5, "cash": -1500}
				},
				{
					"text": "Откладываем до понедельника",
					"effects": {"morale": 2, "reputation": -5, "retention": -3}
				}
			]
		},
		{
			"title": "Конкурент запустил похожую фичу.",
			"options": [
				{
					"text": "Быстро копируем, чтобы не отстать",
					"effects": {"activation": 2, "tech_debt": 4}
				},
				{
					"text": "Упираем на качество и поддержку",
					"effects": {"retention": 3, "reputation": 2}
				}
			]
		}
	]
